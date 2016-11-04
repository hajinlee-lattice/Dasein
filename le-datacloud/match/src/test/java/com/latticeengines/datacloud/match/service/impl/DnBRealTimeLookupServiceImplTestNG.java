package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.exposed.service.DnBRealTimeLookupService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchOutput;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;

public class DnBRealTimeLookupServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(DnBRealTimeLookupServiceImplTestNG.class);

    @Autowired
    private DnBRealTimeLookupService dnBRealTimeLookupService;

    private final static int THREAD_NUM = 20;

    @Test(groups = "functional", dataProvider = "inputData", enabled = true)
    public void testRealTimeLookupService(String name, String city, String state, String country, String message) {
        MatchKeyTuple input = new MatchKeyTuple();
        input.setCountryCode(country);
        input.setName(name);
        input.setState(state);
        input.setCity(city);

        DnBMatchOutput res = dnBRealTimeLookupService.realtimeEntityLookup(input);
        log.info(res.getDuns() + " " + res.getConfidenceCode() + " " + res.getMatchGrade().getRawCode() + " "
                + res.getDnbCode().getMessage());
        Assert.assertEquals(res.getDnbCode().getMessage(), message);
    }

    @Test(groups = "functional", enabled = false)
    public void loadTestRealTimeLookupService() {

        List<String> messageList = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);
        CompletionService<DnBMatchOutput> cs = new ExecutorCompletionService<DnBMatchOutput>(executorService);

        for (int i = 0; i < THREAD_NUM; i++) {
            cs.submit(new Callable<DnBMatchOutput>() {
                public DnBMatchOutput call() throws Exception {
                    MatchKeyTuple input = new MatchKeyTuple();
                    input.setCountryCode("US");
                    input.setName("Gorman Manufacturing");
                    input.setState("CA");

                    DnBMatchOutput res = dnBRealTimeLookupService.realtimeEntityLookup(input);
                    return res;
                }
            });
            log.info("Submit :" + i);
        }

        for (int i = 0; i < THREAD_NUM; i++) {
            try {
                DnBMatchOutput result = cs.take().get();
                log.info(i + " message:" + result.getDnbCode().getMessage());
                messageList.add(result.getDnbCode().getMessage());
            } catch (InterruptedException e) {
                log.error(e);
            } catch (ExecutionException e) {
                log.error(e);
            }
        }

        for(int i = 0; i < THREAD_NUM; i++) {
            if(messageList.get(i).equals(DnBReturnCode.ExceedConcurrentNum.getMessage())) {
                Assert.assertTrue(true);
            }
        }
        Assert.assertTrue(false);
    }

    @DataProvider(name = "inputData")
    public static Object[][] getInputData() {
        return new Object[][] { { "Benchmark Blinds", "Gilbert", "Arizona", "US", DnBReturnCode.OK.getMessage() },
                { "El Camino Machine Welding LLC", "Salinas", "California", "US",
                        DnBReturnCode.OK.getMessage() },
                { "Cornerstone Alliance Church", "Canon City", "Colorado", "US", DnBReturnCode.OK.getMessage() },
                { "  Gorman Manufacturing  ", "", "", "  US  ", DnBReturnCode.DISCARD.getMessage() } };
    }
}
