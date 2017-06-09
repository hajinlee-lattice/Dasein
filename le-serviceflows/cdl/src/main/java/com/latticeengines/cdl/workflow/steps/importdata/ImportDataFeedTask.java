package com.latticeengines.cdl.workflow.steps.importdata;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("importDataFeed")
public class ImportDataFeedTask extends BaseWorkflowStep<ImportDataFeedTaskConfiguration> {

    private static final Log log = LogFactory.getLog(ImportDataFeedTask.class);

    @Autowired
    private EaiProxy eaiProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {
        log.info("Start import data feed task.");
        Long taskId = configuration.getDataFeedTaskId();
        DataFeedTask dataFeedTask = metadataProxy.getDataFeedTask(configuration.getCustomerSpace().toString(), taskId
                .toString());
        importTable(taskId, dataFeedTask);
    }

    private void importTable(Long taskId, DataFeedTask dataFeedTask) {
        EaiJobConfiguration importConfig = setupConfiguration(taskId, dataFeedTask);
        AppSubmission submission = eaiProxy.submitEaiJob(importConfig);
        String applicationId = submission.getApplicationIds().get(0);
        dataFeedTask.setActiveJob(applicationId);
        metadataProxy.updateDataFeedTask(importConfig.getCustomerSpace().toString(), dataFeedTask);
        waitForAppId(applicationId);
    }

    private ImportConfiguration setupConfiguration(Long taskId, DataFeedTask dataFeedTask) {


        ImportConfiguration importConfig = new ImportConfiguration();
        if (dataFeedTask == null) {
            throw new RuntimeException(String.format("Cannot find data feed task for id %s", taskId.toString()));
        }
        String source = dataFeedTask.getSource();
        if (source.equals(SourceType.VISIDB.getName())) {
            importConfig.setCustomerSpace(configuration.getCustomerSpace());
            importConfig.setProperty(ImportProperty.IMPORT_CONFIG_STR, configuration.getImportConfig());
            List<String> identifiers = new ArrayList<>();
            identifiers.add(taskId.toString());
            importConfig.setProperty(ImportProperty.COLLECTION_IDENTIFIERS, JsonUtils.serialize(identifiers));

            SourceImportConfiguration sourceImportConfig = new SourceImportConfiguration();
            sourceImportConfig.setSourceType(SourceType.VISIDB);
            //sourceImportConfig.setTables(configuration.getTables());
            importConfig.addSourceConfiguration(sourceImportConfig);

        } else {
            //todo other type
            throw new RuntimeException(String.format("Source %s not supported!", source));
        }
        return importConfig;
    }
}
