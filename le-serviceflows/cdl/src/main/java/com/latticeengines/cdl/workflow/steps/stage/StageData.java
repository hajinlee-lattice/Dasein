package com.latticeengines.cdl.workflow.steps.stage;

import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("stageData")
public class StageData extends RunDataFlow<StageDataConfiguration> {
}
