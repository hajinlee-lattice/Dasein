package com.latticeengines.quartz.service;

import org.codehaus.jettison.json.JSONObject;

import java.util.concurrent.Callable;

public class TestQuartzJobCallable implements Callable<Boolean> {

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
                JSONObject json = new JSONObject(jobArgument);
                if (json.has("printMsg")) {
                    printArg = json.getString("printMsg");
                }
            }
            System.out.println(String.format("%s, print argument: %s, concurrent num: %d", outputMsg, printArg,
                    concurrentNum));
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
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
