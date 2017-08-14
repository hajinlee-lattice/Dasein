package com.latticeengines.cdl.workflow.steps.importdata;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportConfigurationFactory;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportDataFeedTaskConfiguration;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.eai.EaiJobDetailProxy;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.serviceflows.workflow.report.BaseReportStep;

@Component("importDataFeedTask")
public class ImportDataFeedTask extends BaseReportStep<ImportDataFeedTaskConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ImportDataFeedTask.class);

    @Autowired
    private EaiProxy eaiProxy;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Autowired
    private EaiJobDetailProxy eaiJobDetailProxy;

    @Override
    public void execute() {
        log.info("Start import data feed task.");
        String taskUniqueId = configuration.getDataFeedTaskId();
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(configuration.getCustomerSpace().toString(),
                taskUniqueId);
        importTable(taskUniqueId, dataFeedTask);
        super.execute();
    }

    private void importTable(String taskUniqueId, DataFeedTask dataFeedTask) {
        EaiJobConfiguration importConfig = setupConfiguration(taskUniqueId, dataFeedTask);
        AppSubmission submission = eaiProxy.submitEaiJob(importConfig);
        String applicationId = submission.getApplicationIds().get(0);
        dataFeedTask.setActiveJob(applicationId);
        dataFeedProxy.updateDataFeedTask(importConfig.getCustomerSpace().toString(), dataFeedTask);
        waitForAppId(applicationId);
        EaiImportJobDetail jobDetail = eaiJobDetailProxy.getImportJobDetailByAppId(applicationId);
        getJson().put(dataFeedTask.getEntity(), jobDetail.getProcessedRecords());
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

        return importConfig;
    }

    @Override
    protected ReportPurpose getPurpose() {
        return ReportPurpose.IMPORT_DATA_SUMMARY;
    }
}
