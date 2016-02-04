package com.latticeengines.propdata.api.controller;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.propdata.api.testframework.PropDataApiDeploymentTestNGBase;
import com.latticeengines.propdata.match.testframework.TestMatchInputUtils;
import com.latticeengines.proxy.exposed.propdata.MatchProxy;

@Component
public class MatchResourceLoadDeploymentTestNG extends PropDataApiDeploymentTestNGBase {

    private static Log log = LogFactory.getLog(MatchResourceLoadDeploymentTestNG.class);

    @Autowired
    private MatchProxy matchProxy;

    private static List<List<Object>> accountPool;

    @BeforeClass(groups = "load")
    private void setUp() {
        loadAccountPool();
    }

    @Test(groups = "load", enabled = true, dataProvider = "loadTestDataProvider")
    public void testRealTimeMatchUnderLoad(int numThreads, int numRequests, int accountsPerRequest,
            long durationThreshold) {
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<MatchOutput>> futures = new ArrayList<>();
        for (int i = 0; i < numRequests; i++) {
            Future<MatchOutput> future = executor.submit(new MatchCallable(matchProxy, accountsPerRequest));
            futures.add(future);
        }

        boolean failed = false;
        for (Future<MatchOutput> future : futures) {
            try {
                MatchOutput output = future.get();
                Assert.assertNotNull(output, "Match output should not be null.");
                Assert.assertTrue(output.getStatistics().getTimeElapsedInMsec() <= durationThreshold,
                        "Time elapsed in match, " + output.getStatistics().getTimeElapsedInMsec()
                                + " msec, is longer than the threshold " + durationThreshold);
            } catch (Exception e) {
                failed = true;
                log.error("Failed to get match output.", e);
            }
        }

        Assert.assertFalse(failed, "Test failed, see log above for details.");
    }

    @DataProvider(name = "loadTestDataProvider")
    private Object[][] getLoadTestData() {
        return new Object[][] { { 1, 1, 1, 3000L }, { 1, 1, 10, 3000L }, { 1, 1, 100, 3000L }, { 2, 2, 100, 5000L },
                { 4, 4, 100, 5000L }, { 8, 8, 100, 7500L }, { 16, 16, 100, 15000L }, { 32, 32, 100, 20000L },
                { 64, 64, 100, 25000L } };
    }

    static class MatchCallable implements Callable<MatchOutput> {

        private final int numAccounts;
        private final MatchProxy matchProxy;

        MatchCallable(MatchProxy matchProxy, int numAccounts) {
            this.numAccounts = numAccounts;
            this.matchProxy = matchProxy;
        }

        @Override
        public MatchOutput call() {
            List<List<Object>> data = getGoodAccounts(numAccounts);
            MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data);
            MatchOutput output = matchProxy.match(input, true);
            log.info("[" + Thread.currentThread().getName() + "] " + output.getStatistics().getRowsMatched()
                    + " out of " + output.getStatistics().getRowsRequested() + " accounts are matched, time elapsed = "
                    + output.getStatistics().getTimeElapsedInMsec() + " msec");
            return output;
        }

        private List<List<Object>> getGoodAccounts(int num) {
            int poolSize = accountPool.size();
            List<List<Object>> data = new ArrayList<>();
            Set<Integer> visitedRows = new HashSet<>();
            for (int i = 0; i < num; i++) {
                int randomPos = new Random().nextInt(poolSize);
                while (visitedRows.contains(randomPos)) {
                    randomPos = new Random().nextInt(poolSize);
                }
                data.add(accountPool.get(randomPos));
                visitedRows.add(randomPos);
            }
            return data;
        }
    }

    private static void loadAccountPool() {
        if (accountPool != null) {
            return;
        }

        URL url = Thread.currentThread().getContextClassLoader()
                .getResource("com/latticeengines/propdata/api/controller/GoodMatchInput.csv");
        Assert.assertNotNull(url, "Cannot find GoodMatchInput.csv");

        try {
            CSVParser parser = CSVParser.parse(new File(url.getFile()), Charset.forName("UTF-8"), CSVFormat.EXCEL);
            accountPool = new ArrayList<>();
            boolean firstLine = true;
            int rowNum = 0;
            for (CSVRecord csvRecord : parser) {
                if (firstLine) {
                    firstLine = false;
                    continue;
                }
                List<Object> record = new ArrayList<>(Collections.singleton((Object) rowNum));
                for (String field : csvRecord) {
                    record.add(field);
                }
                accountPool.add(record);

                rowNum++;
            }
            log.info("Loaded " + accountPool.size() + " accounts into account pool.");
        } catch (IOException e) {
            Assert.fail("Failed to load account pool from GoodMatchInput.csv", e);
        }

    }

}
