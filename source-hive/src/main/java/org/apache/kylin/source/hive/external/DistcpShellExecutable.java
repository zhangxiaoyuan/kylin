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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistcpShellExecutable extends ShellExecutable  {
    private static final Logger logger = LoggerFactory.getLogger(DistcpShellExecutable.class);

    private static final String HIVE_NAME = "hiveName";
    private static final String TABLE_NAME = "tableName";
    private static final String OUTPUT_PATH = "output";
    
    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        String tableName = getTableName();
        String hiveName = getHiveName();
        String input = null;
        String database = context.getConfig().getHiveDatabaseForIntermediateTable();
        try {
            if(hiveName == null) {
                return new ExecuteResult(ExecuteResult.State.SUCCEED, "The job is using default hive, do not need copy data!");
            }
            input = HiveManager.getInstance().getHiveTableLocation(database, tableName, hiveName);
        } catch (Exception e) {
            logger.error("Failed to get location of hive table {}.{}, using hive name {}", database, tableName, hiveName);
            return new ExecuteResult(ExecuteResult.State.ERROR , e.getLocalizedMessage());
        }
        String output = getOutputPath();
        logger.info("Copy Intermediate Hive Table input : {} , output : {}", input, output);
        /**
         * Copy hive table only when source hive table location is in different hadoop cluster.
         */
        if(input.startsWith("/") || input.startsWith(HadoopUtil.getCurrentConfiguration().get(FileSystem.FS_DEFAULT_NAME_KEY))) {
            return new ExecuteResult(ExecuteResult.State.SUCCEED, "Hive " + hiveName + " is based on default hadoop cluster, skip copy data .");
        } else {
            Path inputPath = new Path(input);
            input = inputPath.toString();
        }
        String cmd = String.format("hadoop distcp -overwrite %s %s", input, output);
        super.setCmd(cmd);
        
        return super.doWork(context);
    }
    
    public void setHiveName(String name) {
        setParam(HIVE_NAME, name);
    }
    
    public void setTableName(String name) {
        setParam(TABLE_NAME, name);
    }
    
    public void setOutputPath(String output) {
        setParam(OUTPUT_PATH, output);
    }
    
    public String getOutputPath() {
        return getParam(OUTPUT_PATH);
    }
    
    public String getHiveName() {
        return getParam(HIVE_NAME);
    }
    
    public String getTableName() {
        return getParam(TABLE_NAME);
    }
    
    public String getExecCmd() {
        StringBuffer buf = new StringBuffer();
        buf.append(" -").append(HIVE_NAME).append(" ").append(getHiveName()).append(" -").append(TABLE_NAME).append(" ").
            append(getTableName()).append(" -").append(OUTPUT_PATH).append(" ").append(getOutputPath());
        
        return buf.toString();
    }
}
