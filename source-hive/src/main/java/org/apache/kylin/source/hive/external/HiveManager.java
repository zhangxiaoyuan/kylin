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

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.source.hive.HiveClient;
import org.apache.log4j.Logger;


/** 
 * Manager all hive client, default client and external clients.
 * This is a Singleton object.
 * @author hzfengyu
 */
public class HiveManager {
    private volatile static HiveManager HIVE_MANAGER_CACHE = null;
    private static final Logger logger = Logger.getLogger(HiveManager.class);
    private static final String DEFAULT_HIVE_NAME = "default(null)";
    private static final String HIVE_CONFIG_FILE_LOCATION = "conf/hive-site.xml";
    private static final String HIVE_COMMAND_LOCATION = "bin/hive";
    public static final String NOT_SUPPORT_ERROR_MESSAGE = 
            "Do not support external hive, set kylin.external.hive.root.directory to support it";
    
    //hive root directory, if this equals to null or empty meaning just use default hive
    private File externalHiveRootDir = null;
    private ConcurrentHashMap<String, HiveClient> externalHiveMap = null;
    private HiveClient defaultHiveClient = null;
    
    public static HiveManager getInstance() {
        //using default kylin config
        if(HIVE_MANAGER_CACHE == null) {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            synchronized(HiveManager.class) {
                if(HIVE_MANAGER_CACHE == null) {
                    HIVE_MANAGER_CACHE = new HiveManager(config);
                }
            }
        }
        return HIVE_MANAGER_CACHE;
    }
    
    private HiveManager(KylinConfig config) {
        String externalRootDir = config.getExternalHiveRootDirectory();
        if(externalRootDir != null && !externalRootDir.isEmpty()) {
            File file = new File(externalRootDir);
            //check to ensure hive root file exist
            this.externalHiveRootDir = file;
            if(!(file.exists() && file.isDirectory())) {
                logger.warn("Hive root directory " + file.getAbsolutePath() + " do not exist !");
                this.externalHiveRootDir = null;
            }
        } else {
            this.externalHiveRootDir = null;
        }
        this.externalHiveMap = new ConcurrentHashMap<String, HiveClient>();
    }
    
    public List<String> getExternalHiveName() {
        List<String> hiveNames = new LinkedList<String>();
        hiveNames.add(DEFAULT_HIVE_NAME);
        if(this.externalHiveRootDir == null)
            return hiveNames;
        
        //take every diectory in hive root dir is a hive source. take directory name as hive name
        for(File file : this.externalHiveRootDir.listFiles()) {
            if(!file.isDirectory()) {
                logger.warn("File " + file.getAbsolutePath() + " in hive root directory is normal file.");
                continue;
            }
            hiveNames.add(file.getName());
        }
        return hiveNames;
    }
    
    private void checkIsSupportExternalHive() {
        if(this.externalHiveRootDir == null) {
            throw new IllegalArgumentException(NOT_SUPPORT_ERROR_MESSAGE);
        }
    }
    
    public String getHiveConfigFile(String hiveName) {
        if(hiveName == null) {
            return null;
        }
        checkIsSupportExternalHive();
     
        File hiveRootFile = new File(this.externalHiveRootDir, hiveName);
        if(!(hiveRootFile.exists() && hiveRootFile.isDirectory())) {
            throw new IllegalArgumentException("Hive " + hiveName + " root directory " + hiveRootFile.getAbsolutePath() + " do not exist.");
        }
        
        File hiveConfigFile = new File(hiveRootFile, HIVE_CONFIG_FILE_LOCATION);
        if(!(hiveConfigFile.exists() && hiveConfigFile.isFile())) {
            throw new IllegalArgumentException("Hive " + hiveName + " config file " + hiveConfigFile.getAbsolutePath() + " do not exist.");
        }
        return hiveConfigFile.getAbsolutePath();
    }
    
    public HiveClient createHiveClient(String hiveName) {
        //use internal hive client while do not appoint a hive
        if(hiveName == null) { 
            if(defaultHiveClient == null)
                defaultHiveClient = new ExternalHiveClient(null);
            return this.defaultHiveClient;
        }
        checkIsSupportExternalHive();
        
        HiveClient client = this.externalHiveMap.get(hiveName);
        if(client != null)
            return client;
        String configFileLocation = getHiveConfigFile(hiveName);
        if(configFileLocation == null) {
            throw new IllegalArgumentException("Can not find hive " + hiveName + " config file in local hive root directory " +
                    this.externalHiveRootDir.getAbsolutePath());
        }
        
        try {
            client = new ExternalHiveClient(configFileLocation);
            this.externalHiveMap.put(hiveName, client);
        } catch (Exception e) {
            throw new IllegalArgumentException("Can not create hive client for " + hiveName + ", config file " + configFileLocation);
        }
        return client;
    }
    
    public HiveClient createHiveClientWithConfig(Map<String, String> configMap, String hiveName) {
        HiveClient client = this.createHiveClient(hiveName);
        client.appendConfiguration(configMap);
        return client;
    }
    
    public String getHiveCommand(String hiveName) {
        if(hiveName == null) {
            return "hive";
        }
        checkIsSupportExternalHive();
        
        File hiveRootFile = new File(this.externalHiveRootDir, hiveName);
        if(!(hiveRootFile.exists() && hiveRootFile.isDirectory())) {
            throw new IllegalArgumentException("Hive " + hiveName + " root directory " + hiveRootFile.getAbsolutePath() + " do not exist.");
        }
        
        File hiveCmdFile = new File(hiveRootFile, HIVE_COMMAND_LOCATION);
        if(!(hiveCmdFile.exists() && hiveCmdFile.isFile())) {
            throw new IllegalArgumentException("Hive " + hiveName + " bin file " + hiveCmdFile.getAbsolutePath() + " do not exist.");
        }
        return hiveCmdFile.getAbsolutePath();
    }
    
    public String getHiveTableLocation(String database, String tableName, String hiveName) throws Exception {
        HiveClient hiveClient = this.createHiveClient(hiveName);
        try {
            String tableLocation = hiveClient.getHiveTableLocation(database, tableName);
            return tableLocation;
        } catch (Exception e) {
           logger.error("Get hive " + hiveName + " table " + tableName + " location error !");
           throw e;
        }
    }
    
    public String getHiveTableLocation(String fullTableName, String hiveName) throws Exception {
        String[] tables = HadoopUtil.parseHiveTableName(fullTableName);
        String database = tables[0];
        String tableName = tables[1];
        return getHiveTableLocation(database, tableName, hiveName);
    }
    
    public static void clearCache() {
        HIVE_MANAGER_CACHE = null;
        getInstance();
    }
    
    public boolean isSupportExternalHives() {
        return this.externalHiveRootDir != null;
    }
    
    public static boolean isSameHiveSource(String first, String second) {
        if(first == null) {
            return second == null;
        } else {
            return first.equalsIgnoreCase(second);
        }
    }
}
