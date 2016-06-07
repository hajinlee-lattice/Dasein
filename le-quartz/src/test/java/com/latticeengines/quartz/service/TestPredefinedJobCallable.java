package com.latticeengines.quartz.service;

import java.util.concurrent.Callable;

public class TestPredefinedJobCallable implements Callable<Boolean> {

    private String outputMsg;

    @Override
    public Boolean call() throws Exception {
        System.out.println(outputMsg);
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
