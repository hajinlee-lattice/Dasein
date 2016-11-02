package com.latticeengines.dellebi.qbean;

import java.util.concurrent.Callable;

import org.springframework.stereotype.Component;

@Component("dellEbiDailyJob1")
public class DellEbiDailyJob1Bean extends DellEbiDailyJobBean {

    private final String quartzJob = "dellEbiDailyJob1";

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        super.setQuartzJob(quartzJob);
        return super.getCallable(jobArguments);
    }
}
