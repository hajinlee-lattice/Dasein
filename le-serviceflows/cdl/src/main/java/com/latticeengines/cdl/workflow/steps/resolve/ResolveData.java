package com.latticeengines.cdl.workflow.steps.resolve;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.resolve.ResolveDataConfiguration;
import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("resolveData")
public class ResolveData extends RunDataFlow<ResolveDataConfiguration> {
}
