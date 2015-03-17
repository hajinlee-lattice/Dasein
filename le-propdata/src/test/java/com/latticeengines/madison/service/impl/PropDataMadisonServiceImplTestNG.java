package com.latticeengines.madison.service.impl;

import java.util.Date;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.ReflectionUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.propdata.MadisonLogicDailyProgress;
import com.latticeengines.domain.exposed.propdata.MadisonLogicDailyProgressStatus;
import com.latticeengines.madison.entitymanager.PropDataMadisonEntityMgr;
import com.latticeengines.madison.service.PropDataMadisonService;
import com.latticeengines.propdata.service.db.PropDataContext;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:propdata-madison-context.xml",
        "classpath:propdata-madison-properties-context.xml", "classpath:dataflow-context.xml" })
public class PropDataMadisonServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private PropDataMadisonService propDataService;

    @Autowired
    private PropDataMadisonEntityMgr propDataMadisonEntityMgr;

    private String setupProgress(Date date, MadisonLogicDailyProgress dailyProgress, String tableName) throws Exception {
        dailyProgress.setCreateTime(date);
        dailyProgress.setFileDate(date);
        dailyProgress.setDestinationTable(tableName);
        dailyProgress.setStatus(MadisonLogicDailyProgressStatus.DEPIVOTED.getStatus());
        propDataMadisonEntityMgr.create(dailyProgress);
        return ((PropDataMadisonServiceImpl) propDataService).getHdfsDataflowIncrementalRawPathWithDate(date);
    }

//    @Test(groups = "functional")
    public void importFromDB() throws Exception {

        String outputDir1 = null;
        MadisonLogicDailyProgress dailyProgress1 = null;
        String outputDir2 = null;
        MadisonLogicDailyProgress dailyProgress2 = null;

        try {
            Date today = new Date();
            Date yesterday = DateUtils.addDays(today, -1);

            dailyProgress1 = new MadisonLogicDailyProgress();
            outputDir1 = setupProgress(yesterday, dailyProgress1, "MadisonLogicDepivoted_test1");
            downloadFile(dailyProgress1, outputDir1);

            dailyProgress2 = new MadisonLogicDailyProgress();
            outputDir2 = setupProgress(today, dailyProgress2, "MadisonLogicDepivoted_test2");
            downloadFile(dailyProgress2, outputDir2);

        } finally {
            if (outputDir1 != null) {
                // HdfsUtils.rmdir(yarnConfiguration, outputDir1);
            }
            if (dailyProgress1 != null) {
                propDataMadisonEntityMgr.delete(dailyProgress1);
            }
            if (outputDir2 != null) {
                // HdfsUtils.rmdir(yarnConfiguration, outputDir2);
            }
            if (dailyProgress2 != null) {
                propDataMadisonEntityMgr.delete(dailyProgress2);
            }

        }
    }

    private void downloadFile(MadisonLogicDailyProgress dailyProgress, String outputDir) throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, outputDir);
        propDataService.importFromDB(new PropDataContext());

        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, outputDir));
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration,
                ((PropDataMadisonServiceImpl) propDataService).getSuccessFile(outputDir)));
        MadisonLogicDailyProgress newDailyProgress = propDataMadisonEntityMgr.findByKey(dailyProgress);
        Assert.assertEquals(newDailyProgress.getStatus(), MadisonLogicDailyProgressStatus.FINISHED.getStatus());
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration,
                ((PropDataMadisonServiceImpl) propDataService).getSuccessFile(outputDir)));

    }

    @Test(groups = "functional")
    public void transform() throws Exception {

        ReflectionTestUtils.setField(propDataService, "numOfPastDays", 1);
        PropDataContext requestContext = new PropDataContext();
        Date today = new Date();
        String transformOutput = null;
        
        Date yesterday = DateUtils.addDays(today, -1);
        requestContext.setProperty("today", yesterday);
        propDataService.transform(requestContext);
        transformOutput = ((PropDataMadisonServiceImpl) propDataService).getHdfsWorkflowTotalRawPath(yesterday);
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration,
                ((PropDataMadisonServiceImpl) propDataService).getSuccessFile(transformOutput)));
        
        requestContext = new PropDataContext();
        requestContext.setProperty("today", today);
        propDataService.transform(requestContext);
        transformOutput = ((PropDataMadisonServiceImpl) propDataService).getHdfsWorkflowTotalRawPath(today);
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration,
                ((PropDataMadisonServiceImpl) propDataService).getSuccessFile(transformOutput)));
        
        

    }
}
