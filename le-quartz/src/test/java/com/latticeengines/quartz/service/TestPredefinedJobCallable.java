package com.latticeengines.quartz.service;

import java.util.concurrent.Callable;

public class TestPredefinedJobCallable implements Callable<Boolean> {

    private String outputMsg;

    private static int concurrentNum = 0;

    @Override
    public Boolean call() throws Exception {
        if (concurrentNum > 0) {
            throw new Exception("Can't run concurrent job!");
        }
        concurrentNum++;
        System.out.println(String.format("%s, concurrent num: %d", outputMsg, concurrentNum));
        Thread.sleep(1000);
        concurrentNum--;
        return true;
    }

    public TestPredefinedJobCallable(Builder builder) {
        outputMsg = builder.getMsg();
    }

    public static class Builder {

        private String outputMsg;

        public Builder() {

        }

        public Builder outputMsg(String outputMsg) {
            this.outputMsg = outputMsg;
            return this;
        }

        public String getMsg() {
            return outputMsg;
        }
    }

}
