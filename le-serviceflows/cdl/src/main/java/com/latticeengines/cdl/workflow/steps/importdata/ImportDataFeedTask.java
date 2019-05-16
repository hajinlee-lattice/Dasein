package com.latticeengines.cdl.workflow.steps.importdata;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.CSVToHdfsConfiguration;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportConfigurationFactory;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportDataFeedTaskConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("importDataFeedTask")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportDataFeedTask extends BaseWorkflowStep<ImportDataFeedTaskConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ImportDataFeedTask.class);

    @Autowired
    private EaiProxy eaiProxy;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Override
    public void execute() {
        log.info("Start import data feed task.");
        String taskUniqueId = configuration.getDataFeedTaskId();
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(configuration.getCustomerSpace().toString(),
                taskUniqueId);
        importTable(taskUniqueId, dataFeedTask);
    }

    private void importTable(String taskUniqueId, DataFeedTask dataFeedTask) {
        EaiJobConfiguration importConfig = setupConfiguration(taskUniqueId, dataFeedTask);
        AppSubmission submission = eaiProxy.submitEaiJob(importConfig);
        String applicationId = submission.getApplicationIds().get(0);
        dataFeedTask.setActiveJob(applicationId);
        dataFeedProxy.updateDataFeedTask(importConfig.getCustomerSpace().toString(), dataFeedTask);
        saveOutputValue(WorkflowContextConstants.Outputs.EAI_JOB_APPLICATION_ID, applicationId);
        waitForAppId(applicationId);
    }

    private ImportConfiguration setupConfiguration(String taskUniqueId, DataFeedTask dataFeedTask) {
        ImportConfiguration importConfig;
        if (dataFeedTask == null) {
            throw new RuntimeException(String.format("Cannot find data feed task for id %s", taskUniqueId));
        }
        String source = dataFeedTask.getSource();
        SourceType sourceType = SourceType.getByName(source);
        List<String> identifiers = new ArrayList<>();
        importConfig = ImportConfigurationFactory.getImportConfiguration(sourceType, configuration.getImportConfig());

        importConfig.setCustomerSpace(configuration.getCustomerSpace());
        importConfig.setProperty(ImportProperty.IMPORT_CONFIG_STR, configuration.getImportConfig());

        identifiers.add(taskUniqueId);
        importConfig.setProperty(ImportProperty.COLLECTION_IDENTIFIERS, JsonUtils.serialize(identifiers));
        importConfig.setBusinessEntity(BusinessEntity.getByName(dataFeedTask.getEntity()));

        if (sourceType.equals(SourceType.FILE)) {
            putStringValueInContext(WorkflowContextConstants.Outputs.EAI_JOB_INPUT_FILE_PATH,
                    ((CSVToHdfsConfiguration) importConfig).getFilePath());
        }
        return importConfig;
    }

}
