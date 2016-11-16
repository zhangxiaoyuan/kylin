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

package org.apache.kylin.cube.model.validation.rule;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.validation.IValidatorRule;
import org.apache.kylin.cube.model.validation.ResultLevel;
import org.apache.kylin.cube.model.validation.ValidateContext;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 *  find forbid overlaps in each AggregationGroup
 *  the include dims in AggregationGroup must contain all mandatory, hierarchy and joint
 */
public class AggregationGroupRule implements IValidatorRule<CubeDesc> {

    @Override
    public void validate(CubeDesc cube, ValidateContext context) {
        inner(cube, context);
    }

    public AggregationGroupRule() {
    }

    private void inner(CubeDesc cube, ValidateContext context) {

        int index = 0;
        for (AggregationGroup agg : cube.getAggregationGroups()) {
            if (agg.getIncludes() == null) {
                context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " 'includes' field not set");
                continue;
            }

            if (agg.getSelectRule() == null) {
                context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " 'select rule' field not set");
                continue;
            }

            int combination = 1;
            Set<String> includeDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            if (agg.getIncludes() != null) {
                for (String include : agg.getIncludes()) {
                    includeDims.add(include);
                }
            }

            Set<String> mandatoryDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            if (agg.getSelectRule().mandatory_dims != null) {
                for (String m : agg.getSelectRule().mandatory_dims) {
                    mandatoryDims.add(m);
                }
            }

            Set<String> hierarchyDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            if (agg.getSelectRule().hierarchy_dims != null) {
                for (String[] ss : agg.getSelectRule().hierarchy_dims) {
                    for (String s : ss) {
                        hierarchyDims.add(s);
                    }
                    combination = combination * (ss.length + 1);
                }
            }

            Set<String> jointDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            if (agg.getSelectRule().joint_dims != null) {
                for (String[] ss : agg.getSelectRule().joint_dims) {
                    for (String s : ss) {
                        jointDims.add(s);
                    }
                    combination = combination * 2;
                }
            }

            if (!includeDims.containsAll(mandatoryDims) || !includeDims.containsAll(hierarchyDims) || !includeDims.containsAll(jointDims)) {
                List<String> notIncluded = Lists.newArrayList();
                final Iterable<String> all = Iterables.unmodifiableIterable(Iterables.concat(mandatoryDims, hierarchyDims, jointDims));
                for (String dim : all) {
                    if (includeDims.contains(dim) == false) {
                        notIncluded.add(dim);
                    }
                }
                context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " 'includes' dimensions not include all the dimensions:" + notIncluded.toString());
                continue;
            }

            Set<String> normalDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            normalDims.addAll(includeDims);
            normalDims.removeAll(mandatoryDims);
            normalDims.removeAll(hierarchyDims);
            normalDims.removeAll(jointDims);

            combination = combination * (1 << normalDims.size());

            if (CollectionUtils.containsAny(mandatoryDims, hierarchyDims)) {
                Set<String> intersection = new HashSet<>(mandatoryDims);
                intersection.retainAll(hierarchyDims);
                context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " mandatory dimension has overlap with hierarchy dimension: " + intersection.toString());
                continue;
            }
            if (CollectionUtils.containsAny(mandatoryDims, jointDims)) {
                Set<String> intersection = new HashSet<>(mandatoryDims);
                intersection.retainAll(jointDims);
                context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " mandatory dimension has overlap with joint dimension: " + intersection.toString());
                continue;
            }

            int jointDimNum = 0;
            if (agg.getSelectRule().joint_dims != null) {
                for (String[] joints : agg.getSelectRule().joint_dims) {

                    Set<String> oneJoint = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                    for (String s : joints) {
                        oneJoint.add(s);
                    }

                    if (oneJoint.size() < 2) {
                        context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " require at least 2 dimensions in a joint: " + oneJoint.toString());
                        continue;
                    }
                    jointDimNum += oneJoint.size();

                    int overlapHierarchies = 0;
                    if (agg.getSelectRule().hierarchy_dims != null) {
                        for (String[] oneHierarchy : agg.getSelectRule().hierarchy_dims) {
                            Set<String> share = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                            share.addAll(CollectionUtils.intersection(oneJoint, Arrays.asList(oneHierarchy)));

                            if (!share.isEmpty()) {
                                overlapHierarchies++;
                            }
                            if (share.size() > 1) {
                                context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " joint dimensions has overlap with more than 1 dimensions in same hierarchy: " + share.toString());
                                continue;
                            }
                        }

                        if (overlapHierarchies > 1) {
                            context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " joint dimensions has overlap with more than 1 hierarchies");
                            continue;
                        }
                    }
                }

                if (jointDimNum != jointDims.size()) {

                    Set<String> existing = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                    Set<String> overlap = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                    for (String[] joints : agg.getSelectRule().joint_dims) {
                        Set<String> oneJoint = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                        for (String s : joints) {
                            oneJoint.add(s);
                        }
                        if (CollectionUtils.containsAny(existing, oneJoint)) {
                            overlap.addAll(CollectionUtils.intersection(existing, oneJoint));
                        }
                        existing.addAll(oneJoint);
                    }
                    context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " a dimension exists in more than one joint: " + overlap.toString());
                    continue;
                }
            }

            if (combination > getMaxCombinations(cube)) {
                String msg = "Aggregation group " + index + " has too many combinations, current combination is " + combination + ", max allowed combination is " + getMaxCombinations(cube) + "; use 'mandatory'/'hierarchy'/'joint' to optimize; or update 'kylin.cube.aggrgroup.max.combination' to a bigger value.";
                context.addResult(ResultLevel.ERROR, msg);
                continue;
            }

            index++;
        }
    }

    protected int getMaxCombinations(CubeDesc cubeDesc) {
        return cubeDesc.getConfig().getCubeAggrGroupMaxCombination();
    }
}
