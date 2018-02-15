package com.latticeengines.modeling.workflow.steps;

import java.util.Collections;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataflow.FilterReportColumn;
import com.latticeengines.domain.exposed.dataflow.ReportColumn;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.core.dataflow.CreateReportParameters;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.CreatePrematchEventTableReportConfiguration;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.serviceflows.workflow.report.BaseDataFlowReportStep;

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
