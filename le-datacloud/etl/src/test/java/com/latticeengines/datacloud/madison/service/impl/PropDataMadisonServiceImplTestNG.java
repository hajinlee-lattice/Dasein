package com.latticeengines.datacloud.madison.service.impl;

import java.util.Date;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.madison.service.PropDataContext;
import com.latticeengines.datacloud.madison.service.PropDataMadisonService;
import com.latticeengines.domain.exposed.datacloud.MadisonLogicDailyProgress;
import com.latticeengines.domain.exposed.datacloud.MadisonLogicDailyProgressStatus;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-datacloud-etl-madison-context.xml" })
public class PropDataMadisonServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private PropDataMadisonService propDataService;

    @Autowired
    private com.latticeengines.datacloud.madison.entitymanager.PropDataMadisonEntityMgr propDataMadisonEntityMgr;

    private Date today;
    private Date yesterday;
    private String importOutputDir1 = null;
    private MadisonLogicDailyProgress dailyProgress1 = null;
    private String importOutputDir2 = null;
    private MadisonLogicDailyProgress dailyProgress2 = null;

    private String transformOutput1;
    private String transformOutput2;

    @BeforeClass(groups = "madison")
    public void beforeClass() throws Exception {
        today = new Date();
        yesterday = DateUtils.addDays(today, -3);
        today = DateUtils.addDays(today, -2);
        ReflectionTestUtils.setField(propDataService, "numOfPastDays", 1);
        ReflectionTestUtils.setField(propDataService, "numOfOldPastDays", 1);

        dailyProgress1 = new MadisonLogicDailyProgress();
        dailyProgress2 = new MadisonLogicDailyProgress();

        importOutputDir1 = setupProgress(yesterday, dailyProgress1, "MadisonLogicDepivoted_test1", MadisonLogicDailyProgressStatus.DEPIVOTED.getStatus());
        importOutputDir2 = setupProgress(today, dailyProgress2, "MadisonLogicDepivoted_test2", MadisonLogicDailyProgressStatus.FAILED.getStatus());

        transformOutput1 = ((PropDataMadisonServiceImpl) propDataService).getHdfsWorkflowTotalRawPath(yesterday);
        transformOutput2 = ((PropDataMadisonServiceImpl) propDataService).getHdfsWorkflowTotalRawPath(today);

        removeImportHdfsDirs();
        removeTransformHdfsDirs();
    }

    @AfterClass(groups = "madison")
    public void afterClass() throws Exception {
        if (dailyProgress1 != null) {
            propDataMadisonEntityMgr.delete(dailyProgress1);
        }
        if (dailyProgress2 != null) {
            propDataMadisonEntityMgr.delete(dailyProgress2);
        }

//        ((PropDataMadisonServiceImpl) propDataService).cleanupTargetRawData(today);

        removeImportHdfsDirs();
        removeTransformHdfsDirs();

    }

    private void removeTransformHdfsDirs() throws Exception {

        HdfsUtils.rmdir(yarnConfiguration, transformOutput1);
        HdfsUtils.rmdir(yarnConfiguration, transformOutput2);
    }

    private void removeImportHdfsDirs() throws Exception {

        HdfsUtils.rmdir(yarnConfiguration, importOutputDir1);
        HdfsUtils.rmdir(yarnConfiguration, importOutputDir2);
    }

    private String setupProgress(Date date, MadisonLogicDailyProgress dailyProgress, String tableName, String status) throws Exception {
        dailyProgress.setCreateTime(date);
        dailyProgress.setFileDate(date);
        dailyProgress.setDestinationTable(tableName);
        dailyProgress.setStatus(status);
        propDataMadisonEntityMgr.create(dailyProgress);
        return ((PropDataMadisonServiceImpl) propDataService).getHdfsDataflowIncrementalRawPathWithDate(date);
    }

    @Test(groups = "madison")
    public void importFromDB() throws Exception {

        downloadFile(dailyProgress1, importOutputDir1);
        downloadFile(dailyProgress2, importOutputDir2);

    }

    private void downloadFile(MadisonLogicDailyProgress dailyProgress, String outputDir) throws Exception {

        PropDataContext requestContext = new PropDataContext();
        requestContext.setProperty(PropDataMadisonService.RECORD_KEY, dailyProgress);
        PropDataContext responseContext = propDataService.importFromDB(requestContext);

        Assert.assertEquals(responseContext.getProperty(PropDataMadisonService.STATUS_KEY, String.class),
                PropDataMadisonService.STATUS_OK);
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, outputDir));
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration,
                ((PropDataMadisonServiceImpl) propDataService).getSuccessFile(outputDir)));
        MadisonLogicDailyProgress newDailyProgress = propDataMadisonEntityMgr.findByKey(dailyProgress);
        Assert.assertEquals(newDailyProgress.getStatus(), MadisonLogicDailyProgressStatus.FINISHED.getStatus());
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration,
                ((PropDataMadisonServiceImpl) propDataService).getSuccessFile(outputDir)));

    }

    @Test(groups = "madison", dependsOnMethods = "importFromDB")
    public void transform() throws Exception {

        PropDataContext requestContext = new PropDataContext();

        ReflectionTestUtils.setField(propDataService, "fixedDate", yesterday.getDay());
        requestContext.setProperty(PropDataMadisonService.TODAY_KEY, yesterday);
        PropDataContext responseContext = propDataService.transform(requestContext);

        Assert.assertEquals(responseContext.getProperty(PropDataMadisonService.STATUS_KEY, String.class),
                PropDataMadisonService.STATUS_OK);
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration,
                ((PropDataMadisonServiceImpl) propDataService).getSuccessFile(transformOutput1 + "/output")));

        ReflectionTestUtils.setField(propDataService, "fixedDate", today.getDay());
        requestContext = new PropDataContext();
        requestContext.setProperty(PropDataMadisonService.TODAY_KEY, today);
        propDataService.transform(requestContext);
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration,
                ((PropDataMadisonServiceImpl) propDataService).getSuccessFile(transformOutput2 + "/output")));

    }

    @Test(groups = "madison", dependsOnMethods = "transform")
    public void exportToDB() throws Exception {

//        ((PropDataMadisonServiceImpl) propDataService).cleanupTargetRawData(today);

        PropDataContext requestContext = new PropDataContext();
        requestContext.setProperty(PropDataMadisonService.TODAY_KEY, today);
        PropDataContext responseContext = propDataService.exportToDB(requestContext);
        Assert.assertEquals(responseContext.getProperty(PropDataMadisonService.STATUS_KEY, String.class),
                PropDataMadisonService.STATUS_OK);
    }

    // @Test(groups = "manual")
    public void swapTables() {
        String assignedQueue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        ((PropDataMadisonServiceImpl) propDataService).swapTargetTables(assignedQueue);

    }

}
