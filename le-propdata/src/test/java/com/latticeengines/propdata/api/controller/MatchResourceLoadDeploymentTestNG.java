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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.propdata.api.testframework.PropDataApiDeploymentTestNGBase;
import com.latticeengines.propdata.match.testframework.TestMatchInputUtils;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;
import com.latticeengines.proxy.exposed.propdata.MatchProxy;

@Component
public class MatchResourceLoadDeploymentTestNG extends PropDataApiDeploymentTestNGBase {

    private static Log log = LogFactory.getLog(MatchResourceLoadDeploymentTestNG.class);

    @Autowired
    private MatchProxy matchProxy;

    private static List<List<Object>> accountPool;
    private AtomicInteger sharedCounter;

    @BeforeClass(groups = "load")
    private void setUp() {
        loadAccountPool();
        warmup();
        LogManager.getLogger(BaseRestApiProxy.class).setLevel(Level.WARN);
    }

    @Test(groups = "load", dataProvider = "loadTestDataProvider")
    public void testRealTimeMatchUnderLoad(int numRequests, int accountsPerRequest) {
        sharedCounter = new AtomicInteger();
        Integer totalRequests = numRequests * accountsPerRequest;
        ExecutorService executor = Executors.newFixedThreadPool(numRequests);
        List<Future<Long>> futures = new ArrayList<>();
        for (int i = 0; i < numRequests; i++) {
            Future<Long> future = executor
                    .submit(new MatchCallable(matchProxy, accountsPerRequest, sharedCounter, totalRequests));
            futures.add(future);
        }

        boolean failed = false;
        Long totalDuration = 0L;
        for (Future<Long> future : futures) {
            try {
                totalDuration += future.get();
            } catch (Exception e) {
                failed = true;
                log.error("Failed to get match output.", e);
            }
        }

        log.info(String.format("%d threads with %d requests, average duration = %s", numRequests, accountsPerRequest,
                DurationFormatUtils.formatDuration(totalDuration / numRequests, "mm:ss.SSS", failed)));

        Assert.assertFalse(failed, "Test failed, see log above for details.");
    }

    @DataProvider(name = "loadTestDataProvider")
    private Object[][] getLoadTestData() {
        return new Object[][] { { 1, 1 }, { 1, 10 }, { 4, 10 }, { 128, 10 }, { 1024, 10 } };
    }

    private void warmup() {
        List<List<Object>> data = getGoodAccounts(1);
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data);
        matchProxy.matchRealTime(input);
    }

    private class MatchCallable implements Callable<Long> {

        private final Integer numAccounts;
        private final MatchProxy matchProxy;
        private final Integer totalRequest;
        private final AtomicInteger sharedCounter;
        private Integer localCounter = 0;

        MatchCallable(MatchProxy matchProxy, int numAccounts, AtomicInteger sharedCounter, Integer totalRequest) {
            this.numAccounts = numAccounts;
            this.matchProxy = matchProxy;
            this.sharedCounter = sharedCounter;
            this.totalRequest = totalRequest;
        }

        @Override
        public Long call() {
            Long overallStartTime = System.currentTimeMillis();
            List<List<Object>> data = getGoodAccounts(numAccounts);
            for (List<Object> row : data) {
                Long startTime = System.currentTimeMillis();
                MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(Collections.singletonList(row));
                matchProxy.matchRealTime(input);
                localCounter++;
                Long duration = System.currentTimeMillis() - startTime;
                String logMsg = String.format(
                        "Finished %d out of %d request in current thread, %d out of %d globally. Time elapsed = %s",
                        localCounter, numAccounts, sharedCounter.incrementAndGet(), totalRequest,
                        DurationFormatUtils.formatDuration(duration, "mm:ss.SSS", false));
                log.info(logMsg);
            }
            return (System.currentTimeMillis() - overallStartTime) / data.size();
        }
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
