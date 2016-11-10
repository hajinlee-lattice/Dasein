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
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;
import com.latticeengines.datacloud.match.exposed.service.DnBRealTimeLookupService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;

public class DnBRealTimeLookupServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(DnBRealTimeLookupServiceImplTestNG.class);

    @Autowired
    private DnBRealTimeLookupService dnBRealTimeLookupService;

    private final static int THREAD_NUM = 20;

    @Test(groups = "functional", dataProvider = "entityInputData", enabled = true)
    public void testRealTimeEntityLookupService(String name, String city, String state, String country,
            String message) {
        MatchKeyTuple input = new MatchKeyTuple();
        input.setCountryCode(country);
        input.setName(name);
        input.setState(state);
        input.setCity(city);

        DnBMatchContext res = dnBRealTimeLookupService.realtimeEntityLookup(input);
        log.info(res.getDuns() + " " + res.getConfidenceCode() + " " + res.getMatchGrade().getRawCode() + " "
                + res.getDnbCode().getMessage());
        Assert.assertEquals(res.getDnbCode().getMessage(), message);
    }

    @Test(groups = "functional", dataProvider = "emailInputData", enabled = true)
    public void testRealTimeEmailLookupService(String email, String message) {
        MatchKeyTuple input = new MatchKeyTuple();
        input.setEmail(email);

        DnBMatchContext res = dnBRealTimeLookupService.realtimeEmailLookup(input);
        Assert.assertEquals(res.getDnbCode().getMessage(), message);
    }

    @Test(groups = "functional", enabled = false)
    public void loadTestRealTimeLookupService() {

        List<String> messageList = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);
        CompletionService<DnBMatchContext> cs = new ExecutorCompletionService<DnBMatchContext>(executorService);

        for (int i = 0; i < THREAD_NUM; i++) {
            cs.submit(new Callable<DnBMatchContext>() {
                public DnBMatchContext call() throws Exception {
                    MatchKeyTuple input = new MatchKeyTuple();
                    input.setCountryCode("US");
                    input.setName("Gorman Manufacturing");
                    input.setState("CA");

                    DnBMatchContext res = dnBRealTimeLookupService.realtimeEntityLookup(input);
                    return res;
                }
            });
            log.info("Submit :" + i);
        }

        for (int i = 0; i < THREAD_NUM; i++) {
            try {
                DnBMatchContext result = cs.take().get();
                log.info(i + " message:" + result.getDnbCode().getMessage());
                messageList.add(result.getDnbCode().getMessage());
            } catch (InterruptedException e) {
                log.error(e);
            } catch (ExecutionException e) {
                log.error(e);
            }
        }

        for (int i = 0; i < THREAD_NUM; i++) {
            if (messageList.get(i).equals(DnBReturnCode.EXCEED_CONCURRENT_NUM.getMessage())) {
                Assert.assertTrue(true);
            }
        }
        Assert.assertTrue(false);
    }

    @DataProvider(name = "entityInputData")
    public static Object[][] getEntityInputData() {
        return new Object[][] { { "Benchmark Blinds", "Gilbert", "Arizona", "US", DnBReturnCode.OK.getMessage() },
                { "El Camino Machine Welding LLC", "Salinas", "California", "US", DnBReturnCode.OK.getMessage() },
                { "Cornerstone Alliance Church", "Canon City", "Colorado", "US", DnBReturnCode.OK.getMessage() },
                { "  Gorman Manufacturing  ", "", "", "  US  ", DnBReturnCode.DISCARD.getMessage() } };
    }

    @DataProvider(name = "emailInputData")
    public static Object[][] getEmailInputData() {
        return new Object[][] { { "CRISTIANA_MAURICIO@DEACONESS.COM", DnBReturnCode.UNAUTHORIZED.getMessage() } };
    }
}
