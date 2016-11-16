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

package org.apache.kylin.provision;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.job.DeployUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.source.SourcePartition;
import org.apache.kylin.storage.hbase.util.HBaseRegionSizeCalculator;
import org.apache.kylin.storage.hbase.util.ZookeeperJobLock;
import org.apache.kylin.tool.StorageCleanupJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class BuildCubeWithEngine {

    private CubeManager cubeManager;
    private DefaultScheduler scheduler;
    protected ExecutableManager jobService;
    private static boolean fastBuildMode = false;

    private static final Logger logger = LoggerFactory.getLogger(BuildCubeWithEngine.class);

    public static void main(String[] args) throws Exception {
        try {
            beforeClass();

            BuildCubeWithEngine buildCubeWithEngine = new BuildCubeWithEngine();
            buildCubeWithEngine.before();
            buildCubeWithEngine.build();
            buildCubeWithEngine.after();
            logger.info("Build is done");
            afterClass();
            logger.info("Going to exit");
            System.exit(0);
        } catch (Throwable e) {
            logger.error("error", e);
            System.exit(1);
        }
    }

    public static void beforeClass() throws Exception {
        logger.info("Adding to classpath: " + new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        ClassUtil.addClasspath(new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());

        String fastModeStr = System.getProperty("fastBuildMode");
        if (fastModeStr != null && fastModeStr.equalsIgnoreCase("true")) {
            fastBuildMode = true;
            logger.info("Will use fast build mode");
        } else {
            logger.info("Will not use fast build mode");
        }

        System.setProperty(KylinConfig.KYLIN_CONF, HBaseMetadataTestCase.SANDBOX_TEST_DATA);
        if (StringUtils.isEmpty(System.getProperty("hdp.version"))) {
            throw new RuntimeException("No hdp.version set; Please set hdp.version in your jvm option, for example: -Dhdp.version=2.2.4.2-2");
        }

        HBaseMetadataTestCase.staticCreateTestMetadata(HBaseMetadataTestCase.SANDBOX_TEST_DATA);

        try {
            //check hdfs permission
            Configuration hconf = HadoopUtil.getCurrentConfiguration();
            FileSystem fileSystem = FileSystem.get(hconf);
            String hdfsWorkingDirectory = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory();
            Path coprocessorDir = new Path(hdfsWorkingDirectory);
            boolean success = fileSystem.mkdirs(coprocessorDir);
            if (!success) {
                throw new IOException("mkdir fails");
            }
        } catch (IOException e) {
            throw new RuntimeException("failed to create kylin.hdfs.working.dir, Please make sure the user has right to access " + KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory(), e);
        }
    }

    protected void deployEnv() throws IOException {
        DeployUtil.initCliWorkDir();
        DeployUtil.deployMetadata();
        DeployUtil.overrideJobJarLocations();
    }

    public void before() throws Exception {
        deployEnv();

        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        jobService = ExecutableManager.getInstance(kylinConfig);
        scheduler = DefaultScheduler.createInstance();
        scheduler.init(new JobEngineConfig(kylinConfig), new ZookeeperJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        cubeManager = CubeManager.getInstance(kylinConfig);
        for (String jobId : jobService.getAllJobIds()) {
            if (jobService.getJob(jobId) instanceof CubingJob) {
                jobService.deleteJob(jobId);
            }
        }

    }

    public void after() {
        DefaultScheduler.destroyInstance();
    }

    public static void afterClass() {
        HBaseMetadataTestCase.staticCleanupTestMetadata();
    }

    public void build() throws Exception {
        DeployUtil.prepareTestDataForNormalCubes("test_kylin_cube_with_slr_empty");
        KylinConfig.getInstanceFromEnv().setHBaseHFileSizeGB(1.0f);
        testInner();
        testLeft();
        testViewAsLookup();
        KylinConfig.getInstanceFromEnv().setHBaseHFileSizeGB(0.0f);
    }

    protected ExecutableState waitForJob(String jobId) {
        while (true) {
            AbstractExecutable job = jobService.getJob(jobId);
            if (job.getStatus() == ExecutableState.SUCCEED || job.getStatus() == ExecutableState.ERROR) {
                return job.getStatus();
            } else {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void testInner() throws Exception {
        String[] testCase = new String[] { "testInnerJoinCubeWithoutSlr", "testInnerJoinCubeWithSlr" };
        runTestAndAssertSucceed(testCase);
    }

    private void testLeft() throws Exception {
        String[] testCase = new String[] { "testLeftJoinCubeWithSlr", "testLeftJoinCubeWithoutSlr" };
        runTestAndAssertSucceed(testCase);
    }

    private void testViewAsLookup() throws Exception {
        String[] testCase = new String[] { "testInnerJoinCubeWithView", "testLeftJoinCubeWithView" };
        runTestAndAssertSucceed(testCase);
    }

    private void runTestAndAssertSucceed(String[] testCase) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(testCase.length);
        final CountDownLatch countDownLatch = new CountDownLatch(testCase.length);
        List<Future<Boolean>> tasks = Lists.newArrayListWithExpectedSize(testCase.length);
        for (int i = 0; i < testCase.length; i++) {
            tasks.add(executorService.submit(new TestCallable(testCase[i], countDownLatch)));
        }
        countDownLatch.await();
        try {
            for (int i = 0; i < tasks.size(); ++i) {
                Future<Boolean> task = tasks.get(i);
                final Boolean result = task.get();
                if (result == false) {
                    throw new RuntimeException("The test '" + testCase[i] + "' is failed.");
                }
            }
        } catch (Exception ex) {
            logger.error("error", ex);
            throw ex;
        }
    }

    private class TestCallable implements Callable<Boolean> {

        private final String methodName;
        private final CountDownLatch countDownLatch;

        public TestCallable(String methodName, CountDownLatch countDownLatch) {
            this.methodName = methodName;
            this.countDownLatch = countDownLatch;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Boolean call() throws Exception {
            try {
                final Method method = BuildCubeWithEngine.class.getDeclaredMethod(methodName);
                method.setAccessible(true);
                return (Boolean) method.invoke(BuildCubeWithEngine.this);
            } catch (Exception e) {
                logger.error(e.getMessage());
                throw e;
            } finally {
                countDownLatch.countDown();
            }
        }
    }

    private void assertJobSuccess() {

    }

    @SuppressWarnings("unused")
    // called by reflection
    private boolean testInnerJoinCubeWithSlr() throws Exception {
        final String cubeName = "test_kylin_cube_with_slr_empty";
        clearSegment(cubeName);
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        long date1 = 0;
        long date2 = f.parse("2015-01-01").getTime();
        long date3 = f.parse("2022-01-01").getTime();
        List<String> result = Lists.newArrayList();

        if (fastBuildMode) {
            return buildSegment(cubeName, date1, date3);
        } else {
            if (buildSegment(cubeName, date1, date2) == true) {
                return buildSegment(cubeName, date2, date3);//empty segment
            }
        }
        return false;
    }

    @SuppressWarnings("unused")
    // called by reflection
    private Boolean testInnerJoinCubeWithoutSlr() throws Exception {

        final String cubeName = "test_kylin_cube_without_slr_empty";
        clearSegment(cubeName);
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        long date1 = 0;
        long date2 = f.parse("2013-01-01").getTime();
        long date3 = f.parse("2013-07-01").getTime();
        long date4 = f.parse("2022-01-01").getTime();
        List<String> result = Lists.newArrayList();

        if (fastBuildMode) {
            return buildSegment(cubeName, date1, date4);
        } else {
            if (buildSegment(cubeName, date1, date2) == true) {
                if (buildSegment(cubeName, date2, date3) == true) {
                    if (buildSegment(cubeName, date3, date4) == true) {
                        return mergeSegment(cubeName, date1, date3);//don't merge all segments
                    }
                }
            }
        }
        return false;

    }

    @SuppressWarnings("unused")
    // called by reflection
    private boolean testLeftJoinCubeWithoutSlr() throws Exception {
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        List<String> result = Lists.newArrayList();
        final String cubeName = "test_kylin_cube_without_slr_left_join_empty";
        clearSegment(cubeName);

        long date1 = cubeManager.getCube(cubeName).getDescriptor().getPartitionDateStart();
        long date2 = f.parse("2012-06-01").getTime();
        long date3 = f.parse("2022-01-01").getTime();
        long date4 = f.parse("2023-01-01").getTime();

        if (fastBuildMode) {
            return buildSegment(cubeName, date1, date4);
        } else {
            if (buildSegment(cubeName, date1, date2) == true) {
                if (buildSegment(cubeName, date2, date3) == true) {
                    if (buildSegment(cubeName, date3, date4) == true) { //empty segment
                        return mergeSegment(cubeName, date1, date3);//don't merge all segments
                    }
                }
            }
        }

        return false;

    }

    @SuppressWarnings("unused")
    // called by reflection
    private boolean testLeftJoinCubeWithView() throws Exception {
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        List<String> result = Lists.newArrayList();
        final String cubeName = "test_kylin_cube_with_view_left_join_empty";
        clearSegment(cubeName);

        long date1 = cubeManager.getCube(cubeName).getDescriptor().getPartitionDateStart();
        long date4 = f.parse("2023-01-01").getTime();

        return buildSegment(cubeName, date1, date4);

    }

    @SuppressWarnings("unused")
    // called by reflection
    private boolean testInnerJoinCubeWithView() throws Exception {
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        List<String> result = Lists.newArrayList();
        final String cubeName = "test_kylin_cube_with_view_inner_join_empty";
        clearSegment(cubeName);

        long date1 = cubeManager.getCube(cubeName).getDescriptor().getPartitionDateStart();
        long date4 = f.parse("2023-01-01").getTime();

        return buildSegment(cubeName, date1, date4);

    }

    @SuppressWarnings("unused")
    // called by reflection
    private boolean testLeftJoinCubeWithSlr() throws Exception {
        String cubeName = "test_kylin_cube_with_slr_left_join_empty";
        clearSegment(cubeName);

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        long date1 = cubeManager.getCube(cubeName).getDescriptor().getPartitionDateStart();
        long date2 = f.parse("2013-01-01").getTime();
        long date3 = f.parse("2013-07-01").getTime();
        long date4 = f.parse("2022-01-01").getTime();

        List<String> result = Lists.newArrayList();
        if (fastBuildMode) {
            return buildSegment(cubeName, date1, date4);
        } else {
            if (buildSegment(cubeName, date1, date2) == true) {
                if (buildSegment(cubeName, date2, date3) == true) {
                    if (buildSegment(cubeName, date3, date4) == true) {
                        return mergeSegment(cubeName, date1, date3);//don't merge all segments
                    }
                }
            }
        }
        return false;

    }

    private void clearSegment(String cubeName) throws Exception {
        CubeInstance cube = cubeManager.getCube(cubeName);
        // remove all existing segments
        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        cubeBuilder.setToRemoveSegs(cube.getSegments().toArray(new CubeSegment[cube.getSegments().size()]));
        cubeManager.updateCube(cubeBuilder);
    }

    private Boolean mergeSegment(String cubeName, long startDate, long endDate) throws Exception {
        CubeSegment segment = cubeManager.mergeSegments(cubeManager.getCube(cubeName), startDate, endDate, 0, 0, true);
        DefaultChainedExecutable job = EngineFactory.createBatchMergeJob(segment, "TEST");
        jobService.addJob(job);
        ExecutableState state = waitForJob(job.getId());
        return Boolean.valueOf(ExecutableState.SUCCEED == state);
    }

    private Boolean buildSegment(String cubeName, long startDate, long endDate) throws Exception {
        CubeInstance cubeInstance = cubeManager.getCube(cubeName);
        ISource source = SourceFactory.tableSource(cubeInstance);
        SourcePartition partition = source.parsePartitionBeforeBuild(cubeInstance, new SourcePartition(0, endDate, 0, 0, null, null));
        CubeSegment segment = cubeManager.appendSegment(cubeInstance, partition.getStartDate(), partition.getEndDate());
        DefaultChainedExecutable job = EngineFactory.createBatchCubingJob(segment, "TEST");
        jobService.addJob(job);
        ExecutableState state = waitForJob(job.getId());
        //        if (segment.getCubeDesc().getEngineType() == IEngineAware.ID_MR_V1
        //                || segment.getCubeDesc().getStorageType() == IStorageAware.ID_SHARDED_HBASE) {
        //            checkHFilesInHBase(segment);
        //        }
        return Boolean.valueOf(ExecutableState.SUCCEED == state);
    }

    @SuppressWarnings("unused")
    private int cleanupOldStorage() throws Exception {
        String[] args = { "--delete", "true" };

        StorageCleanupJob cli = new StorageCleanupJob();
        cli.execute(args);
        return 0;
    }

    private void checkHFilesInHBase(CubeSegment segment) throws IOException {
        Configuration conf = HBaseConfiguration.create(HadoopUtil.getCurrentConfiguration());
        String tableName = segment.getStorageLocationIdentifier();
        try (HTable table = new HTable(conf, tableName)) {
            HBaseRegionSizeCalculator cal = new HBaseRegionSizeCalculator(table);
            Map<byte[], Long> sizeMap = cal.getRegionSizeMap();
            long totalSize = 0;
            for (Long size : sizeMap.values()) {
                totalSize += size;
            }
            if (totalSize == 0) {
                return;
            }
            Map<byte[], Pair<Integer, Integer>> countMap = cal.getRegionHFileCountMap();
            // check if there's region contains more than one hfile, which means the hfile config take effects
            boolean hasMultiHFileRegions = false;
            for (Pair<Integer, Integer> count : countMap.values()) {
                // check if hfile count is greater than store count
                if (count.getSecond() > count.getFirst()) {
                    hasMultiHFileRegions = true;
                    break;
                }
            }
            if (KylinConfig.getInstanceFromEnv().getHBaseHFileSizeGB() == 0 && hasMultiHFileRegions) {
                throw new IOException("hfile size set to 0, but found region contains more than one hfiles");
            } else if (KylinConfig.getInstanceFromEnv().getHBaseHFileSizeGB() > 0 && !hasMultiHFileRegions) {
                throw new IOException("hfile size set greater than 0, but all regions still has only one hfile");
            }
        }
    }

}