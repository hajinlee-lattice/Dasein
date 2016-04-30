package com.latticeengines.monitor.exposed.metric.stats.impl;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.monitor.exposed.metric.service.StatsService;
import com.latticeengines.monitor.exposed.metric.stats.Inspection;
import com.latticeengines.monitor.metric.measurement.HealthCheck;

public class HealthInspection implements Inspection {

    private static final Long interval = 10000L;

    private String componentName;

    @Autowired
    private StatsService statsService;

    @PostConstruct
    private void postConstruct() {
        this.statsService.register(this);
    }

    public void setComponentName(String componentName) {
        this.componentName = componentName;
    }

    @Override
    public List<Measurement<?, ?>> report() {
        List<Measurement<?, ?>> toReturn = new ArrayList<>();
        toReturn.add(new HealthCheck(componentName));
        return toReturn;
    }

    @Override
    public Long interval() {
        return interval;
    }

    @Override
    public String toString() {
        return "Health Inspection [" + componentName + "] [" + hashCode() + "]";
    }

}
