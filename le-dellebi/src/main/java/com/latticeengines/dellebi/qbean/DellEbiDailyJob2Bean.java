package com.latticeengines.dellebi.qbean;

import java.util.concurrent.Callable;

import org.springframework.stereotype.Component;

@Component("dellEbiDailyJob2")
public class DellEbiDailyJob2Bean extends DellEbiDailyJobBean {

    private final String quartzJob = "dellEbiDailyJob2";

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        super.setQuartzJob(quartzJob);
        return super.getCallable(jobArguments);
    }
}
