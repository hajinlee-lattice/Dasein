package com.latticeengines.cdl.workflow.steps.export;

import java.util.Map;
import java.util.Optional;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.BaseReportStepConfiguration;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.serviceflows.workflow.report.BaseReportStep;

@Component("exportDataToRedshiftReportStep")
public class ExportDataToRedshiftReportStep extends BaseReportStep<BaseReportStepConfiguration> {

    @Override
    public void execute() {
        Map<BusinessEntity, Long> exportReport = getMapObjectFromContext(REDSHIFT_EXPORT_REPORT, BusinessEntity.class,
                Long.class);
        Optional.ofNullable(exportReport).ifPresent(map -> map.forEach((k, v) -> {
            getJson().put(k.getServingStore().name(), v);
        }));
        super.execute();
    }

    @Override
    protected ReportPurpose getPurpose() {
        return ReportPurpose.PUBLISH_DATA_SUMMARY;
    }

}
