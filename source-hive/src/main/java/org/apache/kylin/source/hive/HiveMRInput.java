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

package org.apache.kylin.source.hive;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.LookupDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.hive.external.ExternalHiveClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class HiveMRInput implements IMRInput {

    @Override
    public IMRBatchCubingInputSide getBatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
        return new BatchCubingInputSide(flatDesc);
    }

    @Override
    public IMRTableInputFormat getTableInputFormat(TableDesc table) {
        return new HiveTableInputFormat(table.getIdentity());
    }

    public static class HiveTableInputFormat implements IMRTableInputFormat {
        final String dbName;
        final String tableName;

        /**
         * Construct a HiveTableInputFormat to read hive table.
         * @param fullQualifiedTableName "databaseName.tableName"
         */
        public HiveTableInputFormat(String fullQualifiedTableName) {
            String[] parts = HadoopUtil.parseHiveTableName(fullQualifiedTableName);
            dbName = parts[0];
            tableName = parts[1];
        }

        @Override
        public void configureJob(Job job) {
            // HiveConf config file is static variable and ensure we set correct one. 
            synchronized(ExternalHiveClient.class) {
                HiveConf.setHiveSiteLocation(Thread.currentThread().getContextClassLoader().getResource("hive-site.xml"));
                try {
                    HCatInputFormat.setInput(job, dbName, tableName);
                    job.setInputFormatClass(HCatInputFormat.class);
    
                    job.setMapOutputValueClass(org.apache.hive.hcatalog.data.DefaultHCatRecord.class);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public String[] parseMapperInput(Object mapperInput) {
            return HiveTableReader.getRowAsStringArray((HCatRecord) mapperInput);
        }

    }

    public static class BatchCubingInputSide implements IMRBatchCubingInputSide {

        protected final JobEngineConfig conf;
        protected final IJoinedFlatTableDesc flatDesc;
        protected String hiveViewIntermediateTables = "";

        public BatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
            this.conf = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
            this.flatDesc = flatDesc;
        }

        @Override
        public void addStepPhase1_CreateFlatTable(DefaultChainedExecutable jobFlow) {
            final String cubeName = CubingExecutableUtil.getCubeName(jobFlow.getParams());

            final KylinConfig kylinConfig = CubeManager.getInstance(conf.getConfig()).getCube(cubeName).getConfig();

            String createFlatTableMethod = kylinConfig.getCreateFlatHiveTableMethod();
            if ("1".equals(createFlatTableMethod)) {
                // create flat table first, then count and redistribute
                jobFlow.addTask(createFlatHiveTableStep(conf, flatDesc, jobFlow.getId(), cubeName, false, ""));
                jobFlow.addTask(createRedistributeFlatHiveTableStep(conf, flatDesc, jobFlow.getId(), cubeName));
            } else if ("2".equals(createFlatTableMethod)) {
                // count from source table first, and then redistribute, suitable for partitioned table
                final String rowCountOutputDir = getRowCountOutputDir(conf, jobFlow.getId());
                jobFlow.addTask(createCountHiveTableStep(conf, flatDesc, jobFlow.getId(), rowCountOutputDir));
                jobFlow.addTask(createFlatHiveTableStep(conf, flatDesc, jobFlow.getId(), cubeName, true, rowCountOutputDir));
            } else {
                throw new IllegalArgumentException("Unknown value for kylin.hive.create.flat.table.method: " + createFlatTableMethod);
            }

            AbstractExecutable task = createLookupHiveViewMaterializationStep(jobFlow.getId());
            if (task != null) {
                jobFlow.addTask(task);
            }
        }

        protected String getRowCountOutputDir(JobEngineConfig conf, String jobId) {
            return JobBuilderSupport.getJobWorkingDir(conf, jobId) + "/row_count";
        }

        protected boolean isWriteToLocalDir() {
            return false;
        }

        public AbstractExecutable createRedistributeFlatHiveTableStep(JobEngineConfig conf, IJoinedFlatTableDesc flatTableDesc, String jobId, String cubeName) {
            StringBuilder hiveInitBuf = new StringBuilder();
            hiveInitBuf.append("USE ").append(conf.getConfig().getHiveDatabaseForIntermediateTable()).append(";\n");
            hiveInitBuf.append(JoinedFlatTable.generateHiveSetStatements(conf));

            String rowCountOutputDir = getRowCountOutputDir(conf, jobId);

            RedistributeFlatHiveTableStep step = new RedistributeFlatHiveTableStep();
            step.setHiveName(flatTableDesc.getHiveName());
            step.setInitStatement(hiveInitBuf.toString());
            step.setSelectRowCountStatement(JoinedFlatTable.generateSelectRowCountStatement(flatTableDesc, rowCountOutputDir, isWriteToLocalDir()));
            step.setRowCountOutputDir(rowCountOutputDir);
            step.setRedistributeDataStatement(JoinedFlatTable.generateRedistributeFlatTableStatement(flatTableDesc));
            CubingExecutableUtil.setCubeName(cubeName, step.getParams());
            step.setName(ExecutableConstants.STEP_NAME_REDISTRIBUTE_FLAT_HIVE_TABLE);
            return step;
        }


        public AbstractExecutable createCountHiveTableStep(JobEngineConfig conf, IJoinedFlatTableDesc flatTableDesc, String jobId, String rowCountOutputDir) {
            final ShellExecutable step = new ShellExecutable();
            final String hiveName = flatTableDesc.getHiveName();
            
            final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder(hiveName);
            hiveCmdBuilder.addStatement(JoinedFlatTable.generateHiveSetStatements(conf));
            hiveCmdBuilder.addStatement("set hive.exec.compress.output=false;\n");
            hiveCmdBuilder.addStatement(JoinedFlatTable.generateCountDataStatement(flatTableDesc, rowCountOutputDir, isWriteToLocalDir()));

            step.setCmd(hiveCmdBuilder.build());
            step.setName(ExecutableConstants.STEP_NAME_COUNT_HIVE_TABLE);

            return step;
        }

        public ShellExecutable createLookupHiveViewMaterializationStep(String jobId) {
            ShellExecutable step = new ShellExecutable();
            step.setName(ExecutableConstants.STEP_NAME_MATERIALIZE_HIVE_VIEW_IN_LOOKUP);
            HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder(flatDesc.getHiveName());

            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            MetadataManager metadataManager = MetadataManager.getInstance(kylinConfig);
            final Set<TableDesc> lookupViewsTables = Sets.newHashSet();

            for (LookupDesc lookupDesc : flatDesc.getDataModel().getLookups()) {
                TableDesc tableDesc = metadataManager.getTableDesc(lookupDesc.getTable());
                if (TableDesc.TABLE_TYPE_VIRTUAL_VIEW.equalsIgnoreCase(tableDesc.getTableType())) {
                    lookupViewsTables.add(tableDesc);
                }
            }

            if (lookupViewsTables.size() == 0) {
                return null;
            }
            final String useDatabaseHql = "USE " + conf.getConfig().getHiveDatabaseForIntermediateTable() + ";";
            hiveCmdBuilder.addStatement(useDatabaseHql);
            hiveCmdBuilder.addStatement(JoinedFlatTable.generateHiveSetStatements(conf));
            for (TableDesc lookUpTableDesc : lookupViewsTables) {
                if (TableDesc.TABLE_TYPE_VIRTUAL_VIEW.equalsIgnoreCase(lookUpTableDesc.getTableType())) {
                    StringBuilder createIntermediateTableHql = new StringBuilder();
                    createIntermediateTableHql.append("DROP TABLE IF EXISTS " + lookUpTableDesc.getMaterializedName() + ";\n");
                    createIntermediateTableHql.append("CREATE TABLE IF NOT EXISTS " + lookUpTableDesc.getMaterializedName() + "\n");
                    createIntermediateTableHql.append("LOCATION '" + JobBuilderSupport.getJobWorkingDir(conf, jobId) + "/" + lookUpTableDesc.getMaterializedName() + "'\n");
                    createIntermediateTableHql.append("AS SELECT * FROM " + lookUpTableDesc.getIdentity() + ";\n");
                    hiveCmdBuilder.addStatement(createIntermediateTableHql.toString());
                    hiveViewIntermediateTables = hiveViewIntermediateTables + lookUpTableDesc.getMaterializedName() + ";";
                }
            }

            hiveViewIntermediateTables = hiveViewIntermediateTables.substring(0, hiveViewIntermediateTables.length() - 1);

            step.setCmd(hiveCmdBuilder.build());
            return step;
        }

        public static AbstractExecutable createFlatHiveTableStep(JobEngineConfig conf, IJoinedFlatTableDesc flatTableDesc, String jobId, String cubeName, boolean redistribute, String rowCountOutputDir) {
            StringBuilder hiveInitBuf = new StringBuilder();
            hiveInitBuf.append(JoinedFlatTable.generateHiveSetStatements(conf));

            final String useDatabaseHql = "USE " + conf.getConfig().getHiveDatabaseForIntermediateTable() + ";\n";
            final String dropTableHql = JoinedFlatTable.generateDropTableStatement(flatTableDesc);
            final String createTableHql = JoinedFlatTable.generateCreateTableStatement(flatTableDesc, JobBuilderSupport.getJobWorkingDir(conf, jobId));
            String insertDataHqls = JoinedFlatTable.generateInsertDataStatement(flatTableDesc, conf, redistribute);

            CreateFlatHiveTableStep step = new CreateFlatHiveTableStep();
            step.setHiveName(flatTableDesc.getHiveName());
            step.setUseRedistribute(redistribute);
            step.setInitStatement(hiveInitBuf.toString());
            step.setRowCountOutputDir(rowCountOutputDir);
            step.setCreateTableStatement(useDatabaseHql + dropTableHql + createTableHql + insertDataHqls);
            CubingExecutableUtil.setCubeName(cubeName, step.getParams());
            step.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);
            return step;
        }

        @Override
        public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
            GarbageCollectionStep step = new GarbageCollectionStep();
            step.setName(ExecutableConstants.STEP_NAME_GARBAGE_COLLECTION);
            step.setIntermediateTableIdentity(getIntermediateTableIdentity());
            step.setExternalDataPath(JoinedFlatTable.getTableDir(flatDesc, JobBuilderSupport.getJobWorkingDir(conf, jobFlow.getId())));
            step.setHiveViewIntermediateTableIdentities(hiveViewIntermediateTables);
            jobFlow.addTask(step);
        }

        @Override
        public IMRTableInputFormat getFlatTableInputFormat() {
            return new HiveTableInputFormat(getIntermediateTableIdentity());
        }

        protected String getIntermediateTableIdentity() {
            return conf.getConfig().getHiveDatabaseForIntermediateTable() + "." + flatDesc.getTableName();
        }
    }

    public static class RedistributeFlatHiveTableStep extends AbstractExecutable {
        private final BufferedLogger stepLogger = new BufferedLogger(logger);

        private void computeRowCount(CliCommandExecutor cmdExecutor) throws IOException {
            final String hiveName = getHiveName();
            final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder(hiveName);
            hiveCmdBuilder.addStatement(getInitStatement());
            hiveCmdBuilder.addStatement("set hive.exec.compress.output=false;\n");
            hiveCmdBuilder.addStatement(getSelectRowCountStatement());
            final String cmd = hiveCmdBuilder.build();

            stepLogger.log("Compute row count of flat hive table, cmd: ");
            stepLogger.log(cmd);

            Pair<Integer, String> response = cmdExecutor.execute(cmd, stepLogger);
            if (response.getFirst() != 0) {
                throw new RuntimeException("Failed to compute row count of flat hive table");
            }
        }

        private long readRowCountFromFile(Path file) throws IOException {
            FileSystem fs = FileSystem.get(file.toUri(), HadoopUtil.getCurrentConfiguration());
            InputStream in = fs.open(file);
            try {
                String content = IOUtils.toString(in);
                return Long.valueOf(content.trim()); // strip the '\n' character

            } finally {
                IOUtils.closeQuietly(in);
            }
        }

        private void redistributeTable(KylinConfig config, int numReducers) throws IOException {
            final String hiveName = getHiveName();
            final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder(hiveName);
            hiveCmdBuilder.addStatement(getInitStatement());
            hiveCmdBuilder.addStatement("set mapreduce.job.reduces=" + numReducers + ";\n");
            hiveCmdBuilder.addStatement("set hive.merge.mapredfiles=false;\n");
            hiveCmdBuilder.addStatement(getRedistributeDataStatement());
            final String cmd = hiveCmdBuilder.toString();

            stepLogger.log("Redistribute table, cmd: ");
            stepLogger.log(cmd);

            Pair<Integer, String> response = config.getCliCommandExecutor().execute(cmd, stepLogger);
            if (response.getFirst() != 0) {
                throw new RuntimeException("Failed to redistribute flat hive table");
            }
        }

        private KylinConfig getCubeSpecificConfig() {
            String cubeName = CubingExecutableUtil.getCubeName(getParams());
            CubeManager manager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = manager.getCube(cubeName);
            return cube.getConfig();
        }

        @Override
        protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
            KylinConfig config = getCubeSpecificConfig();

            try {

                computeRowCount(config.getCliCommandExecutor());

                Path rowCountFile = new Path(getRowCountOutputDir(), "000000_0");
                long rowCount = readRowCountFromFile(rowCountFile);
                if (!config.isEmptySegmentAllowed() && rowCount == 0) {
                    stepLogger.log("Detect upstream hive table is empty, " + "fail the job because \"kylin.job.allow.empty.segment\" = \"false\"");
                    return new ExecuteResult(ExecuteResult.State.ERROR, stepLogger.getBufferedLog());
                }

                int mapperInputRows = config.getHadoopJobMapperInputRows();

                int numReducers = Math.round(rowCount / ((float) mapperInputRows));
                numReducers = Math.max(1, numReducers);

                stepLogger.log("total input rows = " + rowCount);
                stepLogger.log("expected input rows per mapper = " + mapperInputRows);
                stepLogger.log("num reducers for RedistributeFlatHiveTableStep = " + numReducers);

                redistributeTable(config, numReducers);
                return new ExecuteResult(ExecuteResult.State.SUCCEED, stepLogger.getBufferedLog());

            } catch (Exception e) {
                logger.error("job:" + getId() + " execute finished with exception", e);
                return new ExecuteResult(ExecuteResult.State.ERROR, stepLogger.getBufferedLog());
            }
        }

        public void setInitStatement(String sql) {
            setParam("HiveInit", sql);
        }

        public String getInitStatement() {
            return getParam("HiveInit");
        }

        public void setSelectRowCountStatement(String sql) {
            setParam("HiveSelectRowCount", sql);
        }

        public String getSelectRowCountStatement() {
            return getParam("HiveSelectRowCount");
        }

        public void setRedistributeDataStatement(String sql) {
            setParam("HiveRedistributeData", sql);
        }

        public String getRedistributeDataStatement() {
            return getParam("HiveRedistributeData");
        }

        public void setRowCountOutputDir(String rowCountOutputDir) {
            setParam("rowCountOutputDir", rowCountOutputDir);
        }

        public String getRowCountOutputDir() {
            return getParam("rowCountOutputDir");
        }
        
        public void setHiveName(String hiveName) {
            setParam("hiveName", hiveName);
        }
        
        public String getHiveName() {
            return getParam("hiveName");
        }
    }

    public static class GarbageCollectionStep extends AbstractExecutable {
        private static final Logger logger = LoggerFactory.getLogger(GarbageCollectionStep.class);

        @Override
        protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
            KylinConfig config = context.getConfig();
            StringBuffer output = new StringBuffer();
            try {
                output.append(cleanUpIntermediateFlatTable(config));
                // don't drop view to avoid concurrent issue
                //output.append(cleanUpHiveViewIntermediateTable(config));
            } catch (IOException e) {
                logger.error("job:" + getId() + " execute finished with exception", e);
                return new ExecuteResult(ExecuteResult.State.ERROR, e.getMessage());
            }

            return new ExecuteResult(ExecuteResult.State.SUCCEED, output.toString());
        }

        protected String cleanUpIntermediateFlatTable(KylinConfig config) throws IOException {
            StringBuffer output = new StringBuffer();
            final String hiveTable = this.getIntermediateTableIdentity();
            if (config.isHiveKeepFlatTable() == false && StringUtils.isNotEmpty(hiveTable)) {
                final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
                hiveCmdBuilder.addStatement("USE " + config.getHiveDatabaseForIntermediateTable() + ";");
                hiveCmdBuilder.addStatement("DROP TABLE IF EXISTS  " + hiveTable + ";");

                config.getCliCommandExecutor().execute(hiveCmdBuilder.build());
                output.append("Hive table " + hiveTable + " is dropped. \n");

                rmdirOnHDFS(getExternalDataPath());
                output.append("Hive table " + hiveTable + " external data path " + getExternalDataPath() + " is deleted. \n");
            }
            return output.toString();
        }

        private void mkdirOnHDFS(String path) throws IOException {
            Path externalDataPath = new Path(path);
            FileSystem fs = FileSystem.get(externalDataPath.toUri(), HadoopUtil.getCurrentConfiguration());
            if (!fs.exists(externalDataPath)) {
                fs.mkdirs(externalDataPath);
            }
        }

        private void rmdirOnHDFS(String path) throws IOException {
            Path externalDataPath = new Path(path);
            FileSystem fs = FileSystem.get(externalDataPath.toUri(), HadoopUtil.getCurrentConfiguration());
            if (fs.exists(externalDataPath)) {
                fs.delete(externalDataPath, true);
            }
        }

        private String cleanUpHiveViewIntermediateTable(KylinConfig config) throws IOException {
            StringBuffer output = new StringBuffer();
            final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
            hiveCmdBuilder.addStatement("USE " + config.getHiveDatabaseForIntermediateTable() + ";");
            if (getHiveViewIntermediateTableIdentities() != null && !getHiveViewIntermediateTableIdentities().isEmpty()) {
                for (String hiveTableName : getHiveViewIntermediateTableIdentities().split(";")) {
                    hiveCmdBuilder.addStatement("DROP TABLE IF EXISTS  " + hiveTableName + ";");
                }
            }
            config.getCliCommandExecutor().execute(hiveCmdBuilder.build());
            output.append("hive view intermediate tables: " + getHiveViewIntermediateTableIdentities() + " is dropped. \n");
            return output.toString();
        }

        public void setIntermediateTableIdentity(String tableIdentity) {
            setParam("oldHiveTable", tableIdentity);
        }

        protected String getIntermediateTableIdentity() {
            return getParam("oldHiveTable");
        }

        public void setExternalDataPath(String externalDataPath) {
            setParam("externalDataPath", externalDataPath);
        }

        protected String getExternalDataPath() {
            return getParam("externalDataPath");
        }

        public void setHiveViewIntermediateTableIdentities(String tableIdentities) {
            setParam("oldHiveViewIntermediateTables", tableIdentities);
        }

        protected String getHiveViewIntermediateTableIdentities() {
            return getParam("oldHiveViewIntermediateTables");
        }
        
        public String getCmd() {
            StringBuffer buf = new StringBuffer();
            buf.append(" -").append("externalDataPath").append(" ").append(getIntermediateTableIdentity()).append(" -").append("externalDataPath").append(" ").
                append(getExternalDataPath()).append(" -").append("oldHiveViewIntermediateTables").append(" ").append(getHiveViewIntermediateTableIdentities());
            
            return buf.toString();
        }
    }

}
