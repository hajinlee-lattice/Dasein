package com.latticeengines.cdl.workflow.steps.export;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.BaseReportStepConfiguration;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.serviceflows.workflow.report.BaseReportStep;

@Component("exportDataToRedshiftReportStep")
public class ExportDataToRedshiftReportStep extends BaseReportStep<BaseReportStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportDataToRedshiftReportStep.class);

    @Override
    public void execute() {
        Map<BusinessEntity, Table> entityTableMap = getMapObjectFromContext(TABLE_GOING_TO_REDSHIFT,
                BusinessEntity.class, Table.class);
        if (entityTableMap != null && !entityTableMap.isEmpty()) {
            entityTableMap.forEach((k, v) -> //
                    getJson().put(k.getServingStore().name(), v.getExtracts().get(0).getProcessedRecords()));
            super.execute();
        } else {
            log.info("No table is going to redshift, skip report.");
        }
    }

    @Override
    protected ReportPurpose getPurpose() {
        return ReportPurpose.PUBLISH_DATA_SUMMARY;
    }

}
