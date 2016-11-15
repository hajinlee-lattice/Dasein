package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
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
import com.latticeengines.datacloud.match.dnb.DnBMatchGrade;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;
import com.latticeengines.datacloud.match.service.DnBRealTimeLookupService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;

public class DnBRealTimeLookupServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(DnBRealTimeLookupServiceImplTestNG.class);

    @Autowired
    private DnBRealTimeLookupService dnBRealTimeLookupService;

    private final static int THREAD_NUM = 20;
    


    @Test(groups = "functional", dataProvider = "entityInputData", enabled = true)
    public void testRealTimeEntityLookupService(String name, String city, String state, String country,
            DnBReturnCode dnbCode, String duns, Integer ConfidenceCode, DnBMatchGrade matchGrade) {
        MatchKeyTuple input = new MatchKeyTuple();
        input.setCountryCode(country);
        input.setName(name);
        input.setState(state);
        input.setCity(city);
        DnBMatchContext context = new DnBMatchContext();
        context.setInputNameLocation(input);
        context.setLookupRequestId(UUID.randomUUID().toString());

        DnBMatchContext res = dnBRealTimeLookupService.realtimeEntityLookup(context);
        Assert.assertEquals(res.getDnbCode(), dnbCode);
        Assert.assertEquals(res.getDuns(), duns);
        Assert.assertEquals(res.getConfidenceCode(), ConfidenceCode);
        Assert.assertEquals(res.getMatchGrade(), matchGrade);
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
                    DnBMatchContext context = new DnBMatchContext();
                    context.setInputNameLocation(input);
                    return dnBRealTimeLookupService.realtimeEntityLookup(context);
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

        boolean flag = false;
        for (int i = 0; i < THREAD_NUM; i++) {
            if (messageList.get(i).equals(DnBReturnCode.EXCEED_CONCURRENT_NUM.getMessage())) {
                flag = true;
            }
        }
        Assert.assertTrue(flag);
    }

    @Test(groups = "functional", dataProvider = "emailInputData", enabled = true)
    public void testRealTimeEmailLookupService(String email, DnBReturnCode dnbCode, String duns) {
        DnBMatchContext context = new DnBMatchContext();
        context.setInputEmail(email);
        DnBMatchContext res = dnBRealTimeLookupService.realtimeEmailLookup(context);
        Assert.assertEquals(res.getDnbCode(), dnbCode);
        Assert.assertEquals(res.getDuns(), duns);
    }

    @DataProvider(name = "entityInputData")
    public static Object[][] getEntityInputData() {
        return new Object[][] {
                { "Benchmark Blinds", "Gilbert", "Arizona", "US", DnBReturnCode.OK, "038796548", 8,
                        new DnBMatchGrade("AZZAAZZZFAB") },
                { "Désirée Daude", null, null, "DE", DnBReturnCode.BAD_REQUEST, null, null, null },
                { "ABCDEFG", "New York", "Washinton", "US", DnBReturnCode.UNMATCH, null, null, null },
                { "Gorman Manufacturing", null, null, "US", DnBReturnCode.DISCARD, null, 6,
                        new DnBMatchGrade("AZZZZZZZFZZ") } };
    }

    @DataProvider(name = "emailInputData")
    public static Object[][] getEmailInputData() {
        return new Object[][] { { "CRISTIANA_MAURICIO@DEACONESS.COM", DnBReturnCode.UNMATCH, null },
                { "JREMLEY@GOOGLE.COM", DnBReturnCode.OK, "060902413" } };
    }
}
