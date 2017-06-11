package com.latticeengines.cdl.workflow.steps.stage;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.stage.StageDataConfiguration;
import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("stageData")
public class StageData extends RunDataFlow<StageDataConfiguration> {
}
