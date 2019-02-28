package com.latticeengines.aws.firehose.impl;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.aws.firehose.FirehoseService;
import com.latticeengines.common.exposed.timer.PerformanceTimer;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-aws-context.xml" })
public class FirehoseServiceImplTestNG extends AbstractTestNGSpringContextTests {

    private static final int BATCH_SIZE = 1_000;

    @Inject
    private FirehoseService firehoseService;

    private static String format = "{\"ticker_symbol\":\"Lattice%s\",\"sector\":\"HEALTHCARE\",\"change\":-0.8,\"price\":152.83}";

    @BeforeClass(groups = "manual")
    public void setup() {
    }

    @AfterClass(groups = "manual")
    public void teardown() {
    }

    @Test(groups = "manual")
    public void send() {

        try (PerformanceTimer timer = new PerformanceTimer("Perform-Single")) {
            for (int i = 0; i < BATCH_SIZE; i++) {
                String data = String.format(format, i);
                firehoseService.send("latticeengines-etl-score-history-dev", data);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test(groups = "manual")
    public void sendBatch() {
        List<String> streams = new ArrayList<>();
        try (PerformanceTimer timer = new PerformanceTimer("Perform-Batch")) {
            for (int i = 0; i < BATCH_SIZE; i++) {
                String data = String.format(format, "-batch-" + i);
                streams.add(data);
            }
            firehoseService.sendBatch("latticeengines-etl-score-history-dev", streams);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
