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
package org.apache.kylin.source.hive.external;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.source.hive.CreateFlatHiveTableStep;
import org.apache.kylin.source.hive.HiveCmdBuilder;
import org.apache.kylin.source.hive.HiveMRInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * External hive file mr input
 * @author hzfengyu
 */
public class ExternalHiveMRInput extends HiveMRInput {
    @Override
    public IMRBatchCubingInputSide getBatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
        return new BatchFileCubingInputSide(flatDesc);
    }
    
    public static class BatchFileCubingInputSide extends BatchCubingInputSide {
        private static final Logger logger = LoggerFactory.getLogger(BatchFileCubingInputSide.class);

        public BatchFileCubingInputSide(IJoinedFlatTableDesc flatDesc) {
            super(flatDesc);
        }

        @Override
        public void addStepPhase1_CreateFlatTable(DefaultChainedExecutable jobFlow) {
            super.addStepPhase1_CreateFlatTable(jobFlow);
            final String cubeName = CubingExecutableUtil.getCubeName(jobFlow.getParams());

            /**
             * Create table as flat hive table in default hive the same as external hive table, use it in next steps.
             * Do not create view in default table. because kylin read lookup table file.
             */
            jobFlow.addTask(createFlatTableInDefaultHive(conf, flatDesc, jobFlow.getId(), cubeName));
            AbstractExecutable copyDataStep = createCopyHiveDataStep(flatDesc.getTableName(), flatDesc.getHiveName(),
                    JoinedFlatTable.getTableDir(flatDesc, JobBuilderSupport.getJobWorkingDir(conf, jobFlow.getId())));
            if(copyDataStep != null) {
                jobFlow.addTask(copyDataStep);
            }
        }
        
        @Override
        protected String getRowCountOutputDir(JobEngineConfig conf, String jobId) {
            String tempDir = System.getProperty("java.io.tmpdir", "/tmp");
            return String.format("file://%s/kylin-%s/%s", tempDir, jobId, "/row_count");
        }
        
        @Override
        protected boolean isWriteToLocalDir() {
            return true;
        }
        
        protected AbstractExecutable createCopyHiveDataStep(String flatHiveTableName, String hiveName, String output) {
            DistcpShellExecutable copyHiveTableSetp = new DistcpShellExecutable();
            copyHiveTableSetp.setName(ExecutableConstants.STEP_NAME_COPY_HIVE_DATA);
            copyHiveTableSetp.setHiveName(hiveName);
            copyHiveTableSetp.setOutputPath(output);
            copyHiveTableSetp.setTableName(flatHiveTableName);

            return copyHiveTableSetp;
        }
        
        protected AbstractExecutable createFlatTableInDefaultHive(JobEngineConfig conf, IJoinedFlatTableDesc flatTableDesc, String jobId, String cubeName) {
            StringBuilder hiveInitBuf = new StringBuilder();
            hiveInitBuf.append(JoinedFlatTable.generateHiveSetStatements(conf));

            final String useDatabaseHql = "USE " + conf.getConfig().getHiveDatabaseForIntermediateTable() + ";\n";
            final String dropTableHql = JoinedFlatTable.generateDropTableStatement(flatTableDesc);
            final String createTableHql = JoinedFlatTable.generateCreateTableStatement(flatTableDesc, JobBuilderSupport.getJobWorkingDir(conf, jobId));

            CreateFlatHiveTableStep step = new CreateFlatHiveTableStep();
            step.setHiveName(null);
            step.setUseRedistribute(false);
            step.setInitStatement(hiveInitBuf.toString());
            step.setRowCountOutputDir(null);
            step.setCreateTableStatement(useDatabaseHql + dropTableHql + createTableHql);
            CubingExecutableUtil.setCubeName(cubeName, step.getParams());
            step.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE_IN_DEFAULT);
            return step;
        }
        
        public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
            String hiveName = flatDesc.getHiveName();
            
            ExternalGarbageCollectionStep step = new ExternalGarbageCollectionStep();
            step.setName(ExecutableConstants.STEP_NAME_GARBAGE_COLLECTION);
            step.setIntermediateTableIdentity(getIntermediateTableIdentity());
            step.setExternalDataPath(JoinedFlatTable.getTableDir(flatDesc, JobBuilderSupport.getJobWorkingDir(conf, jobFlow.getId())));
            step.setHiveViewIntermediateTableIdentities(hiveViewIntermediateTables);
            step.setHiveName(hiveName);
            jobFlow.addTask(step);
        }
        
        public static class ExternalGarbageCollectionStep extends GarbageCollectionStep {
            @Override
            protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
                KylinConfig config = context.getConfig();
                StringBuffer output = new StringBuffer();
                try {
                    output.append(super.cleanUpIntermediateFlatTable(config));
                    output.append(cleanUpExternalHiveFlatTable(config));
                    // don't drop view to avoid concurrent issue
                    //output.append(cleanUpHiveViewIntermediateTable(config));
                } catch (IOException e) {
                    logger.error("job:" + getId() + " execute finished with exception", e);
                    return new ExecuteResult(ExecuteResult.State.ERROR, e.getMessage());
                }

                return new ExecuteResult(ExecuteResult.State.SUCCEED, output.toString());
            }
            
            public String cleanUpExternalHiveFlatTable(KylinConfig config) throws IOException {
                StringBuffer output = new StringBuffer();
                String hiveName = this.getHiveName();
                if(hiveName == null)
                    return output.toString();
                final String hiveTable = getIntermediateTableIdentity();
                if (StringUtils.isNotEmpty(hiveTable)) {
                    final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder(hiveName);
                    hiveCmdBuilder.addStatement("USE " + config.getHiveDatabaseForIntermediateTable() + ";");
                    hiveCmdBuilder.addStatement("DROP TABLE IF EXISTS  " + hiveTable + ";");
                    
                    //remove table location first, otherwise table will not be found.
                    String rmExternalOutput = rmExternalTableDirOnHDFS(hiveTable, hiveName);
                    config.getCliCommandExecutor().execute(hiveCmdBuilder.build());
                    output.append("Hive table " + hiveTable + " is dropped. \n");
                    output.append(rmExternalOutput);
                }
                return output.toString();
            }
            
            private String rmExternalTableDirOnHDFS(String tableName, String hive) throws IOException {
                try {
                    String dir = HiveManager.getInstance().getHiveTableLocation(tableName, hive);
                    Path path = new Path(dir);
                    FileSystem fs = HadoopUtil.getFileSystem(dir);
                    if(fs.exists(path)) {
                        fs.delete(path, true);
                    }
                    return "Remove External Hive " + hive + " Table " + tableName + ", location " + path.toString() + "\n";
                } catch (Exception e) {
                    logger.warn("Get table localtion failed ! table {}, hive name {}.", tableName, hive , e);
                    return "Fetch external table location or delete path failed. skip it.";
                }
            }
            
            public void setHiveName(String hiveName) {
                setParam("hiveName", hiveName);
            }
            
            public String getHiveName() {
                return getParam("hiveName");
            }
            
            @Override
            public String getCmd() {
                StringBuffer sb = new StringBuffer(super.getCmd());
                sb.append(" -").append("hiveName").append(" ").append(this.getHiveName());
                return sb.toString();
            }
        }
    }

}