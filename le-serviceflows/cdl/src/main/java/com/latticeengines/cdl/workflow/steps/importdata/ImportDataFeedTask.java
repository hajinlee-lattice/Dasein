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
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportDataFeedTaskConfiguration;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("importDataFeed")
public class ImportDataFeedTask extends BaseWorkflowStep<ImportDataFeedTaskConfiguration> {

    private static final Log log = LogFactory.getLog(ImportDataFeedTask.class);

    @Autowired
    private EaiProxy eaiProxy;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Override
    public void execute() {
        log.info("Start import data feed task.");
        String taskUniqueId = configuration.getDataFeedTaskId();
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(configuration.getCustomerSpace().toString(), taskUniqueId);
        importTable(taskUniqueId, dataFeedTask);
    }

    private void importTable(String taskUniqueId, DataFeedTask dataFeedTask) {
        EaiJobConfiguration importConfig = setupConfiguration(taskUniqueId, dataFeedTask);
        AppSubmission submission = eaiProxy.submitEaiJob(importConfig);
        String applicationId = submission.getApplicationIds().get(0);
        dataFeedTask.setActiveJob(applicationId);
        dataFeedProxy.updateDataFeedTask(importConfig.getCustomerSpace().toString(), dataFeedTask);
        waitForAppId(applicationId);
    }

    private ImportConfiguration setupConfiguration(String taskUniqueId, DataFeedTask dataFeedTask) {
        ImportConfiguration importConfig = new ImportConfiguration();
        if (dataFeedTask == null) {
            throw new RuntimeException(String.format("Cannot find data feed task for id %s", taskUniqueId));
        }
        String source = dataFeedTask.getSource();
        if (source.equals(SourceType.VISIDB.getName())) {
            importConfig.setCustomerSpace(configuration.getCustomerSpace());
            importConfig.setProperty(ImportProperty.IMPORT_CONFIG_STR, configuration.getImportConfig());
            List<String> identifiers = new ArrayList<>();
            identifiers.add(taskUniqueId);
            importConfig.setProperty(ImportProperty.COLLECTION_IDENTIFIERS, JsonUtils.serialize(identifiers));

            SourceImportConfiguration sourceImportConfig = new SourceImportConfiguration();
            sourceImportConfig.setSourceType(SourceType.VISIDB);
            importConfig.addSourceConfiguration(sourceImportConfig);

        } else {
            //todo other type
            throw new RuntimeException(String.format("Source %s not supported!", source));
        }
        return importConfig;
    }
}
