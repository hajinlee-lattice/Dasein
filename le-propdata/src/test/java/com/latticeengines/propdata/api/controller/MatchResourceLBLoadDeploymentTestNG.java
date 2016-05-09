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
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.SSLUtils;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.propdata.api.testframework.PropDataApiDeploymentTestNGBase;
import com.latticeengines.propdata.match.testframework.TestMatchInputUtils;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;
import com.latticeengines.proxy.exposed.propdata.MatchProxy;

@Component
public class MatchResourceLBLoadDeploymentTestNG extends PropDataApiDeploymentTestNGBase {

    private static Log log = LogFactory.getLog(MatchResourceLBLoadDeploymentTestNG.class);

    @Autowired
    private MatchProxy matchProxy;

    private static List<List<Object>> accountPool;
    private Long baseLine;

    private static List<String> summary = new ArrayList<>();

    private String tc1Url = getenv("PROPDATA_TC_1", "http://10.41.0.24:8080", String.class);
    private String tc2Url = getenv("PROPDATA_TC_2", "http://10.41.0.28:8080", String.class);
    private String svipUrl = getenv("PROPDATA_SVIP", "http://10.41.0.26:8080", String.class);
    private Integer concurrency = getenv("PROPDATA_CONCURRENCY", 64, Integer.class);

    @BeforeClass(groups = "load.temp")
    private void setUp() {
        loadAccountPool();
        LogManager.getLogger(BaseRestApiProxy.class).setLevel(Level.WARN);
    }

    @AfterClass(groups = "load.temp")
    private void tearDown() {
        log.info("Summary:\n" + StringUtils.join(summary, "\n"));
    }

    @Test(groups = "load.temp", dataProvider = "loadTestDataProvider")
    public void testRealTimeMatchUnderLoad(int numThreads, int accountsPerRequest, String msHostPort) {
        if (StringUtils.isEmpty(msHostPort)) {
            log.info("Skip test case due to empty msHostPort");
            return;
        }

        log.info("Change microservice host port to " + msHostPort);
        matchProxy.setMicroserviceHostPort(msHostPort);
        warmUp();

        summary.add("\nUsing host port: " + msHostPort);

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        AtomicInteger sharedCounter = new AtomicInteger();
        Integer totalRequests = numThreads * accountsPerRequest;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<Long>> futures = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
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

        stopWatch.stop();
        String msg = String.format("Finished %d requests with in %d msec, throughput = %.2f request per second",
                totalRequests, stopWatch.getTime(), (totalRequests * 1000.0 / stopWatch.getTime()));
        summary.add(msg);
        log.info(msg);

        Long avgDuration = totalDuration / numThreads;

        if (numThreads == 1) {
            baseLine = avgDuration;
            msg = String.format("%d threads with %d requests, average duration = %s, set the baseline to %s",
                    numThreads, accountsPerRequest, duration(avgDuration), duration(baseLine));
            summary.add(msg);
            log.info(msg);
        } else {
            Double ratio = new Double(avgDuration) / new Double(baseLine);
            msg = String.format(
                    "%d threads with %d requests, average duration = %s, which is %.2f times of the baseline",
                    numThreads, accountsPerRequest, duration(avgDuration), ratio);
            summary.add(msg);
            log.info(msg);
        }

        Assert.assertFalse(failed, "Test failed, see log above for details.");
    }

    @DataProvider(name = "loadTestDataProvider")
    private Object[][] getLoadTestData() {
        return new Object[][] { { 1, 1, svipUrl }, { concurrency, 10, svipUrl }, { 1, 1, tc1Url }, { concurrency, 10, tc1Url },
                { 1, 1, tc2Url }, { concurrency, 10, tc2Url } };
    }

    private void warmUp() {
        SSLUtils.turnOffSslChecking();
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
                SSLUtils.turnOffSslChecking();
                matchProxy.matchRealTime(input);
                localCounter++;
                Long duration = System.currentTimeMillis() - startTime;
                String logMsg = String.format(
                        "Finished %d out of %d request in current thread, %d out of %d globally. Time elapsed = %s",
                        localCounter, numAccounts, sharedCounter.incrementAndGet(), totalRequest, duration(duration));
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

    private String duration(Long msec) {
        return DurationFormatUtils.formatDuration(msec, "mm:ss.SSS", false);
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

    private <T> T getenv(String variable, T dflt, Class<T> clazz) {
        String value = System.getenv(variable);
        log.info(variable + ": " + value);
        if (value == null) {
            return dflt;
        }
        try {
            return (T) clazz.getConstructor(new Class[] { String.class }).newInstance(value);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to parse %s as a %s", value, clazz.getSimpleName()));
        }
    }

}
