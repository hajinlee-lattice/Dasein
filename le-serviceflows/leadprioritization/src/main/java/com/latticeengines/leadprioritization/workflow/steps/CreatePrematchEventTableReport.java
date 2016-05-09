package com.latticeengines.leadprioritization.workflow.steps;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataflow.FilterReportColumn;
import com.latticeengines.domain.exposed.dataflow.ReportColumn;
import com.latticeengines.domain.exposed.dataflow.flows.CreateReportParameters;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.serviceflows.workflow.report.BaseDataFlowReportStep;

import edu.emory.mathcs.backport.java.util.Collections;

@Component("createPrematchEventTableReport")
public class CreatePrematchEventTableReport extends BaseDataFlowReportStep<CreatePrematchEventTableReportConfiguration> {

    @Override
    protected ReportPurpose getPurpose() {
        return ReportPurpose.PREMATCH_EVENT_TABLE_SUMMARY;
    }

    @Override
    @SuppressWarnings("unchecked")
    public CreateReportParameters getDataFlowParameters() {
        CreateReportParameters parameters = new CreateReportParameters();
        parameters.columns.add(new ReportColumn("count", "COUNT"));
        parameters.columns.add(new FilterReportColumn("events", InterfaceName.Event.toString(), //
                Collections.singletonList(InterfaceName.Event.toString()), "COUNT"));
        parameters.sourceTableName = configuration.getSourceTableName();
        return parameters;
    }
}
