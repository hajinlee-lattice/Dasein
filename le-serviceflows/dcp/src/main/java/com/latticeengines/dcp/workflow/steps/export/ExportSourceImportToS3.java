package com.latticeengines.dcp.workflow.steps.export;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.dcp.steps.DCPExportStepConfiguration;
import com.latticeengines.serviceflows.workflow.export.BaseImportExportS3;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("exportSourceImportToS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportSourceImportToS3 extends BaseImportExportS3<DCPExportStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportSourceImportToS3.class);

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        
    }
}
