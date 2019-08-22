package com.latticeengines.dataflow.service.impl.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import cascading.flow.Flow;
import cascading.flow.FlowListener;

@SuppressWarnings("rawtypes")
@Component("dataFlowListener")
public class DataFlowListener implements FlowListener {
    private static final Logger log = LoggerFactory.getLogger(DataFlowListener.class);

    @Override
    public void onStarting(Flow flow) {
        log.info(String.format("Starting flow %s.", flow.getName()));
    }

    @Override
    public void onStopping(Flow flow) {
        log.info(String.format("Stopping flow %s.", flow.getName()));
    }

    @Override
    public void onCompleted(Flow flow) {
        log.info(String.format("Flow %s completed.", flow.getName()));
    }

    @Override
    public boolean onThrowable(Flow flow, Throwable throwable) {
        log.warn(String.format("Exception during execution of flow %s.", flow.getName()), throwable);
        return false;
    }

}
