package com.latticeengines.actors.exposed;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.domain.exposed.actors.MeasurementMessage;
import com.latticeengines.monitor.exposed.metric.service.MetricService;

@Component("metricActor")
@Scope("prototype")
public class MetricActor extends ActorTemplate {
    private static final Logger log = LoggerFactory.getLogger(MetricActor.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Autowired
    private MetricService metricService;

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof MeasurementMessage;
    }

    @SuppressWarnings("unchecked")
    protected void processMessage(Object msg) {
        MeasurementMessage message = (MeasurementMessage) msg;
        metricService.write(message.getMetricDB(), message.getMeasurements());
    }

}
