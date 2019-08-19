package com.latticeengines.dataflow.service.impl.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowStep;
import cascading.flow.FlowStepListener;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.DataFlowStepJob;

@SuppressWarnings("rawtypes")
public class DataFlowStepListener implements FlowStepListener {
    private static final Logger log = LoggerFactory.getLogger(DataFlowStepListener.class);

    @Override
    public void onStepStarting(FlowStep flowStep) {
        log.info(String.format("Starting flow step %s.", flowStep.getStepDisplayName()));
        DataFlowStepJob stepJob = new DataFlowStepJob(((BaseFlowStep) flowStep).getFlowStepJob());
        log.info(String.format("Application id for step %s= %s", //
                flowStep.getStepDisplayName(), //
                stepJob.getJobId().replace("job", "application")));
    }

    @Override
    public void onStepStopping(FlowStep flowStep) {
        log.info(String.format("Stopping flow step %s.", flowStep.getStepDisplayName()));
    }

    @Override
    public void onStepRunning(FlowStep flowStep) {
        log.info(String.format("Flow step %s has started running.", flowStep.getStepDisplayName()));
    }

    @Override
    public void onStepCompleted(FlowStep flowStep) {
        log.info(String.format("Flow step %s has completed.", flowStep.getStepDisplayName()));
        log.info(String.format("Stats = %s.", flowStep.getFlowStepStats().toString()));
    }

    @Override
    public boolean onStepThrowable(FlowStep flowStep, Throwable throwable) {
        log.warn(String.format("Exception during step %s.", flowStep.getStepDisplayName()), throwable);
        return false;
    }

}
