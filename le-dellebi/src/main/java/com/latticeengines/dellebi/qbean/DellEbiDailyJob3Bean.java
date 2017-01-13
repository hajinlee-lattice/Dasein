package com.latticeengines.dellebi.qbean;

import java.util.concurrent.Callable;

import org.springframework.stereotype.Component;

@Component("dellEbiDailyJob3")
public class DellEbiDailyJob3Bean extends DellEbiDailyJobBean {

    private final String quartzJob = "dellEbiDailyJob3";

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        super.setQuartzJob(quartzJob);
        return super.getCallable(jobArguments);
    }
}
