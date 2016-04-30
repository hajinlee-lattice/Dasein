package com.latticeengines.monitor.exposed.metric.stats.impl;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.latticeengines.monitor.exposed.metric.service.StatsService;
import com.latticeengines.monitor.exposed.metric.stats.Inspection;

public class ThreadPoolInspection extends AbstractIntegerInspection implements Inspection {

    @Autowired
    @Qualifier("monitorScheduler")
    private ThreadPoolTaskScheduler scheduler;

    @Autowired
    private StatsService statsService;

    private ThreadPoolTaskExecutor taskExecutor;

    public void setTaskExecutor(ThreadPoolTaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    @PostConstruct
    private void postConstruct() {
        this.setScheduler(scheduler);
        this.bootstrap();
        statsService.register(this);
    }

    @Override
    protected Integer getCurrentValue() {
        return taskExecutor.getActiveCount();
    }

    @Override
    public String toString() {
        return "ThreadPool Inspection [" + inspectionName + "] [" + hashCode() + "]";
    }

}
