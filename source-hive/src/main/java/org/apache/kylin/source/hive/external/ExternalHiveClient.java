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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.kylin.source.hive.HiveClient;

/**
 * Hive meta API client for external hive
 * @author hzfengyu
 */
public class ExternalHiveClient extends HiveClient {
    private final static String LOCAL_FS_SCHEMA = "file://";
    
    public ExternalHiveClient(String location) {
        URL uri = null;
        if(location != null) {
            try {
                uri = new URL(LOCAL_FS_SCHEMA + location);
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException("Can not find hive config file " + location);
            }
        } else {
            uri = Thread.currentThread().getContextClassLoader().getResource("hive-site.xml");
        }
        
        /**
         * In HiveConf, hiveSiteURL is a static variable, so we should use a global lock.
         * If uri is null, HiveConf will use the file from java classpath.
         */
        synchronized(ExternalHiveClient.class) {
            hiveConf.setHiveSiteLocation(uri);
            hiveConf = new HiveConf(HiveClient.class);
        }
    }
    
    public ExternalHiveClient(Map<String, String> configMap, String location) {
        this(location);
        appendConfiguration(configMap);
    }
    
    @Override
    protected HiveMetaStoreClient getMetaStoreClient() throws Exception {
        /**
         * HMSHandler is a LocalThread variable, in tomcat we should check it.
         * When to remove hive meta store client in thread local variable:
         * 1: when create new hive client. but HMSHandler exist in current thread.
         * 2: when change hive client in current thread
         */
        if (metaStoreClient == null) {
            HiveMetaStore.HMSHandler.removeRawStore();
            metaStoreClient = new HiveMetaStoreClient(hiveConf);
        } else if(HiveMetaStore.HMSHandler.getRawStore() != null && HiveMetaStore.HMSHandler.getRawStore().getConf() != this.hiveConf) {
            HiveMetaStore.HMSHandler.getRawStore().shutdown();
            HiveMetaStore.HMSHandler.getRawStore().setConf(this.hiveConf);
        }
        return metaStoreClient;
    }
}
