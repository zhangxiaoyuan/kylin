/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.storage.gtrecord;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.RawQueryLastHacker;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeDesc.DeriveInfo;
import org.apache.kylin.dict.lookup.LookupStringTable;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.translate.DerivedFilterTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public abstract class GTCubeStorageQueryBase implements IStorageQuery {

    private static final Logger logger = LoggerFactory.getLogger(GTCubeStorageQueryBase.class);

    private final CubeInstance cubeInstance;
    private final CubeDesc cubeDesc;

    public GTCubeStorageQueryBase(CubeInstance cube) {
        this.cubeInstance = cube;
        this.cubeDesc = cube.getDescriptor();
    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest, TupleInfo returnTupleInfo) {

        //deal with participant columns in subquery join
        sqlDigest.includeSubqueryJoinParticipants();

        //cope with queries with no aggregations
        RawQueryLastHacker.hackNoAggregations(sqlDigest, cubeDesc);

        // Customized measure taking effect: e.g. allow custom measures to help raw queries
        notifyBeforeStorageQuery(sqlDigest);

        Collection<TblColRef> groups = sqlDigest.groupbyColumns;
        TupleFilter filter = sqlDigest.filter;

        // build dimension & metrics
        Set<TblColRef> dimensions = new LinkedHashSet<TblColRef>();
        Set<FunctionDesc> metrics = new LinkedHashSet<FunctionDesc>();
        buildDimensionsAndMetrics(sqlDigest, dimensions, metrics);

        // all dimensions = groups + other(like filter) dimensions
        Set<TblColRef> otherDims = Sets.newHashSet(dimensions);
        otherDims.removeAll(groups);

        // expand derived (xxxD means contains host columns only, derived columns were translated)
        Set<TblColRef> derivedPostAggregation = Sets.newHashSet();
        Set<TblColRef> groupsD = expandDerived(groups, derivedPostAggregation);
        Set<TblColRef> otherDimsD = expandDerived(otherDims, derivedPostAggregation);
        otherDimsD.removeAll(groupsD);

        // identify cuboid
        Set<TblColRef> dimensionsD = new LinkedHashSet<TblColRef>();
        dimensionsD.addAll(groupsD);
        dimensionsD.addAll(otherDimsD);
        Cuboid cuboid = Cuboid.identifyCuboid(cubeDesc, dimensionsD, metrics);
        logger.info("Cuboid identified: cube={}, cuboidId={}, groupsD={}, otherDimsD={}", cubeInstance.getName(), cuboid.getId(), groupsD, otherDimsD);
        context.setCuboid(cuboid);

        // set whether to aggr at storage
        Set<TblColRef> singleValuesD = findSingleValueColumns(filter);
        context.setNeedStorageAggregation(isNeedStorageAggregation(cuboid, groupsD, singleValuesD));

        // replace derived columns in filter with host columns; columns on loosened condition must be added to group by
        Set<TblColRef> loosenedColumnD = Sets.newHashSet();
        TupleFilter filterD = translateDerived(filter, loosenedColumnD);
        groupsD.addAll(loosenedColumnD);

        // set limit push down
        enableStorageLimitIfPossible(cuboid, groups, derivedPostAggregation, groupsD, filter, loosenedColumnD, sqlDigest.aggregations, context);
        // set cautious threshold to prevent out of memory
        setThresholdIfNecessary(dimensionsD, metrics, context);

        List<CubeSegmentScanner> scanners = Lists.newArrayList();
        for (CubeSegment cubeSeg : cubeInstance.getSegments(SegmentStatusEnum.READY)) {
            CubeSegmentScanner scanner;
            try {
                scanner = new CubeSegmentScanner(cubeSeg, cuboid, dimensionsD, groupsD, metrics, filterD, context, getGTStorage());
            } catch (IllegalArgumentException ex) {
                // ref KYLIN-1967, real empty segment can trigger dictionary exception -- IllegalArgumentException: Value not exists!
                if (cubeSeg.getInputRecords() == 0) {
                    logger.warn("cube segment {} input record is 0, skip it still", cubeSeg);
                    continue;
                }
                throw ex;
            }
            scanners.add(scanner);
        }

        if (scanners.isEmpty())
            return ITupleIterator.EMPTY_TUPLE_ITERATOR;

        return new SequentialCubeTupleIterator(scanners, cuboid, dimensionsD, metrics, returnTupleInfo, context);
    }

    protected abstract String getGTStorage();

    private void buildDimensionsAndMetrics(SQLDigest sqlDigest, Collection<TblColRef> dimensions, Collection<FunctionDesc> metrics) {
        for (FunctionDesc func : sqlDigest.aggregations) {
            if (!func.isDimensionAsMetric()) {
                // use the FunctionDesc from cube desc as much as possible, that has more info such as HLLC precision
                metrics.add(findAggrFuncFromCubeDesc(func));
            }
        }

        for (TblColRef column : sqlDigest.allColumns) {
            // skip measure columns
            if (sqlDigest.metricColumns.contains(column) && !(sqlDigest.groupbyColumns.contains(column) || sqlDigest.filterColumns.contains(column))) {
                continue;
            }

            dimensions.add(column);
        }
    }

    private FunctionDesc findAggrFuncFromCubeDesc(FunctionDesc aggrFunc) {
        for (MeasureDesc measure : cubeDesc.getMeasures()) {
            if (measure.getFunction().equals(aggrFunc))
                return measure.getFunction();
        }
        return aggrFunc;
    }

    private Set<TblColRef> expandDerived(Collection<TblColRef> cols, Set<TblColRef> derivedPostAggregation) {
        Set<TblColRef> expanded = Sets.newHashSet();
        for (TblColRef col : cols) {
            if (cubeDesc.hasHostColumn(col)) {
                DeriveInfo hostInfo = cubeDesc.getHostInfo(col);
                for (TblColRef hostCol : hostInfo.columns) {
                    expanded.add(hostCol);
                    if (hostInfo.isOneToOne == false)
                        derivedPostAggregation.add(hostCol);
                }
            } else {
                expanded.add(col);
            }
        }
        return expanded;
    }

    @SuppressWarnings("unchecked")
    private Set<TblColRef> findSingleValueColumns(TupleFilter filter) {
        Collection<? extends TupleFilter> toCheck;
        if (filter instanceof CompareTupleFilter) {
            toCheck = Collections.singleton(filter);
        } else if (filter instanceof LogicalTupleFilter && filter.getOperator() == FilterOperatorEnum.AND) {
            toCheck = filter.getChildren();
        } else {
            return (Set<TblColRef>) Collections.EMPTY_SET;
        }

        Set<TblColRef> result = Sets.newHashSet();
        for (TupleFilter f : toCheck) {
            if (f instanceof CompareTupleFilter) {
                CompareTupleFilter compFilter = (CompareTupleFilter) f;
                // is COL=const ?
                if (compFilter.getOperator() == FilterOperatorEnum.EQ && compFilter.getValues().size() == 1 && compFilter.getColumn() != null) {
                    result.add(compFilter.getColumn());
                }
            }
        }

        // expand derived
        Set<TblColRef> resultD = Sets.newHashSet();
        for (TblColRef col : result) {
            if (cubeDesc.isExtendedColumn(col)) {
                throw new CubeDesc.CannotFilterExtendedColumnException(col);
            }
            if (cubeDesc.isDerived(col)) {
                DeriveInfo hostInfo = cubeDesc.getHostInfo(col);
                if (hostInfo.isOneToOne) {
                    for (TblColRef hostCol : hostInfo.columns) {
                        resultD.add(hostCol);
                    }
                }
                //if not one2one, it will be pruned
            } else {
                resultD.add(col);
            }
        }
        return resultD;
    }

    public boolean isNeedStorageAggregation(Cuboid cuboid, Collection<TblColRef> groupD, Collection<TblColRef> singleValueD) {

        logger.info("GroupD :" + groupD);
        logger.info("SingleValueD :" + singleValueD);
        logger.info("Cuboid columns :" + cuboid.getColumns());

        HashSet<TblColRef> temp = Sets.newHashSet();
        temp.addAll(groupD);
        temp.addAll(singleValueD);
        if (cuboid.getColumns().size() == temp.size()) {
            logger.info("Does not need storage aggregation");
            return false;
        } else {
            logger.info("Need storage aggregation");
            return true;
        }
    }

    @SuppressWarnings("unchecked")
    private TupleFilter translateDerived(TupleFilter filter, Set<TblColRef> collector) {
        if (filter == null)
            return filter;

        if (filter instanceof CompareTupleFilter) {
            return translateDerivedInCompare((CompareTupleFilter) filter, collector);
        }

        List<TupleFilter> children = (List<TupleFilter>) filter.getChildren();
        List<TupleFilter> newChildren = Lists.newArrayListWithCapacity(children.size());
        boolean modified = false;
        for (TupleFilter child : children) {
            TupleFilter translated = translateDerived(child, collector);
            newChildren.add(translated);
            if (child != translated)
                modified = true;
        }
        if (modified) {
            filter = replaceChildren(filter, newChildren);
        }
        return filter;
    }

    private TupleFilter replaceChildren(TupleFilter filter, List<TupleFilter> newChildren) {
        if (filter instanceof LogicalTupleFilter) {
            LogicalTupleFilter r = new LogicalTupleFilter(filter.getOperator());
            r.addChildren(newChildren);
            return r;
        } else
            throw new IllegalStateException("Cannot replaceChildren on " + filter);
    }

    private TupleFilter translateDerivedInCompare(CompareTupleFilter compf, Set<TblColRef> collector) {
        if (compf.getColumn() == null || compf.getValues().isEmpty())
            return compf;

        TblColRef derived = compf.getColumn();
        if (cubeDesc.isExtendedColumn(derived)) {
            throw new CubeDesc.CannotFilterExtendedColumnException(derived);
        }
        if (cubeDesc.isDerived(derived) == false)
            return compf;

        DeriveInfo hostInfo = cubeDesc.getHostInfo(derived);
        CubeManager cubeMgr = CubeManager.getInstance(this.cubeInstance.getConfig());
        CubeSegment seg = cubeInstance.getLatestReadySegment();
        LookupStringTable lookup = cubeMgr.getLookupTable(seg, hostInfo.dimension);
        Pair<TupleFilter, Boolean> translated = DerivedFilterTranslator.translate(lookup, hostInfo, compf);
        TupleFilter translatedFilter = translated.getFirst();
        boolean loosened = translated.getSecond();
        if (loosened) {
            collectColumnsRecursively(translatedFilter, collector);
        }
        return translatedFilter;
    }

    private void collectColumnsRecursively(TupleFilter filter, Set<TblColRef> collector) {
        if (filter == null)
            return;

        if (filter instanceof ColumnTupleFilter) {
            collector.add(((ColumnTupleFilter) filter).getColumn());
        }
        for (TupleFilter child : filter.getChildren()) {
            collectColumnsRecursively(child, collector);
        }
    }

    private void setThresholdIfNecessary(Collection<TblColRef> dimensions, Collection<FunctionDesc> metrics, StorageContext context) {
        boolean hasMemHungryMeasure = false;
        for (FunctionDesc func : metrics) {
            hasMemHungryMeasure |= func.getMeasureType().isMemoryHungry();
        }

        // need to limit the memory usage for memory hungry measures
        if (hasMemHungryMeasure == false) {
            return;
        }

        int rowSizeEst = dimensions.size() * 3;
        for (FunctionDesc func : metrics) {
            // FIXME getStorageBytesEstimate() is not appropriate as here we want size in memory (not in storage)
            rowSizeEst += func.getReturnDataType().getStorageBytesEstimate();
        }

        long rowEst = this.cubeInstance.getConfig().getQueryMemBudget() / rowSizeEst;
        if (rowEst > 0) {
            logger.info("Memory budget is set to " + rowEst + " rows");
            context.setThreshold((int) rowEst);
        } else {
            logger.info("Memory budget is not set.");
        }
    }

    private void enableStorageLimitIfPossible(Cuboid cuboid, Collection<TblColRef> groups, Set<TblColRef> derivedPostAggregation, Collection<TblColRef> groupsD, TupleFilter filter, Set<TblColRef> loosenedColumnD, Collection<FunctionDesc> functionDescs, StorageContext context) {
        boolean possible = true;

        if (!TupleFilter.isEvaluableRecursively(filter)) {
            possible = false;
            logger.info("Storage limit push down is impossible because the filter isn't evaluable");
        }

        if (!loosenedColumnD.isEmpty()) { // KYLIN-2173
            possible = false;
            logger.info("Storage limit push down is impossible because filter is loosened: " + loosenedColumnD);
        }

        if (context.hasSort()) {
            possible = false;
            logger.info("Storage limit push down is impossible because the query has order by");
        }

        // derived aggregation is bad, unless expanded columns are already in group by
        if (!groups.containsAll(derivedPostAggregation)) {
            possible = false;
            logger.info("Storage limit push down is impossible because derived column require post aggregation: " + derivedPostAggregation);
        }

        //if groupsD is clustered at "head" of the rowkey, then limit push down is possible
        int size = groupsD.size();
        if (!groupsD.containsAll(cuboid.getColumns().subList(0, size))) {
            possible = false;
            logger.info("Storage limit push down is impossible because groupD is not clustered at head, groupsD: " + groupsD //
                    + " with cuboid columns: " + cuboid.getColumns());
        }

        //if exists measures like max(cal_dt), then it's not a perfect cuboid match, cannot apply limit
        for (FunctionDesc functionDesc : functionDescs) {
            if (functionDesc.isDimensionAsMetric()) {
                possible = false;
                logger.info("Storage limit push down is impossible because {} isDimensionAsMetric ", functionDesc);
            }
        }

        if (possible) {
            context.enableLimit();
            context.setFinalPushDownLimit(cubeInstance);
        }
    }

    private void notifyBeforeStorageQuery(SQLDigest sqlDigest) {
        Map<String, List<MeasureDesc>> map = Maps.newHashMap();
        for (MeasureDesc measure : cubeDesc.getMeasures()) {
            MeasureType<?> measureType = measure.getFunction().getMeasureType();

            String key = measureType.getClass().getCanonicalName();
            List<MeasureDesc> temp = null;
            if ((temp = map.get(key)) != null) {
                temp.add(measure);
            } else {
                map.put(key, Lists.<MeasureDesc> newArrayList(measure));
            }
        }

        for (List<MeasureDesc> sublist : map.values()) {
            sublist.get(0).getFunction().getMeasureType().adjustSqlDigest(sublist, sqlDigest);
        }
    }

}
