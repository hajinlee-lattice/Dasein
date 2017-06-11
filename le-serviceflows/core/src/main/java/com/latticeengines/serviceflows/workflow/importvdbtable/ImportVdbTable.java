package com.latticeengines.serviceflows.workflow.importvdbtable;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportVdbTableStepConfiguration;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("importVdbTable")
public class ImportVdbTable extends BaseWorkflowStep<ImportVdbTableStepConfiguration> {

    private static final Log log = LogFactory.getLog(ImportVdbTable.class);

    @Autowired
    private EaiProxy eaiProxy;

    @Override
    public void execute() {
        log.info("Start import Vdb table.");
        importVdbTable();
    }

    private void importVdbTable() {
        EaiJobConfiguration importConfig = setupConfiguration();
        AppSubmission submission = eaiProxy.submitEaiJob(importConfig);
        String applicationId = submission.getApplicationIds().get(0);
        waitForAppId(applicationId);
    }

    private ImportConfiguration setupConfiguration() {
        ImportConfiguration importConfig = new ImportConfiguration();

        importConfig.setCustomerSpace(configuration.getCustomerSpace());
        importConfig.setProperty(ImportProperty.IMPORT_CONFIG_STR, configuration.getImportConfigurationStr());
        List<String> identifiers = new ArrayList<>();
        identifiers.add(configuration.getCollectionIdentifier());
        importConfig.setProperty(ImportProperty.COLLECTION_IDENTIFIERS, JsonUtils.serialize(identifiers));

        SourceImportConfiguration sourceImportConfig = new SourceImportConfiguration();
        sourceImportConfig.setSourceType(SourceType.VISIDB);
        //sourceImportConfig.setTables(configuration.getTables());
        importConfig.addSourceConfiguration(sourceImportConfig);
        return importConfig;
    }
}
