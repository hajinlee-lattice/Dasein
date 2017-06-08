package com.latticeengines.serviceflows.workflow.report;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.core.steps.DataFlowStepConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component
public class RunCreateReportDataFlow extends RunDataFlow<DataFlowStepConfiguration> {
}
