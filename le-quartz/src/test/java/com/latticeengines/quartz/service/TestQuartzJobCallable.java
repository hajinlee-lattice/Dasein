package com.latticeengines.quartz.service;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestQuartzJobCallable implements Callable<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(TestQuartzJobCallable.class);

    private String outputMsg;

    private String jobArgument;

    private static int concurrentNum = 0;

    @Override
    public Boolean call() throws Exception {
        if (concurrentNum > 0) {
            throw new Exception("Can't run concurrent job!");
        }
        concurrentNum++;
        try {
            String printArg = null;
            if (jobArgument != null && !jobArgument.isEmpty()) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readValue(jobArgument, JsonNode.class);
                if (jsonNode.has("printMsg")) {
                    printArg = jsonNode.get("printMsg").textValue();
                }
            }
            log.info(String.format("%s, print argument: %s, concurrent num: %d", outputMsg, printArg, concurrentNum));
            Thread.sleep(1000);
        } catch (Exception e) {
            log.error("Failed to print job info.", e);
        } finally {
            concurrentNum--;
        }
        return true;
    }

    public TestQuartzJobCallable(Builder builder) {
        outputMsg = builder.getMsg();
        jobArgument = builder.getJobArgument();

    }

    public static class Builder {

        private String outputMsg;

        private String jobArgument;

        public Builder() {

        }

        public Builder outputMsg(String outputMsg) {
            this.outputMsg = outputMsg;
            return this;
        }

        public Builder jobArgument(String jobArgument) {
            this.jobArgument = jobArgument;
            return this;
        }

        public String getMsg() {
            return outputMsg;
        }

        public String getJobArgument() {
            return jobArgument;
        }
    }

}
