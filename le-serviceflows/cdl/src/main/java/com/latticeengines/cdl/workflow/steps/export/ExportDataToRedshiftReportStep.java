package com.latticeengines.cdl.workflow.steps.export;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.BaseReportStepConfiguration;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.serviceflows.workflow.report.BaseReportStep;

@Component("exportDataToRedshiftReportStep")
public class ExportDataToRedshiftReportStep extends BaseReportStep<BaseReportStepConfiguration> {

    @Override
    public void execute() {
        Map<BusinessEntity, Table> entityTableMap = getMapObjectFromContext(TABLE_GOING_TO_REDSHIFT,
                BusinessEntity.class, Table.class);
        entityTableMap.forEach((k, v) -> {
            getJson().put("Published_" + k.name(), v.getExtracts().get(0).getProcessedRecords());
        });
        super.execute();
    }

    @Override
    protected ReportPurpose getPurpose() {
        return ReportPurpose.PUBLISH_DATA_SUMMARY;
    }

}
