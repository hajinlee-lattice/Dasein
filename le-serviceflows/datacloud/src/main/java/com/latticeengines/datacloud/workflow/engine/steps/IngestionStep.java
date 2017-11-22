package com.latticeengines.datacloud.workflow.engine.steps;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProgressService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProviderService;
import com.latticeengines.domain.exposed.datacloud.ingestion.ProviderConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.IngestionStepConfiguration;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.yarn.exposed.service.JobService;

@Component("ingestionStep")
@Scope("prototype")
public class IngestionStep extends BaseWorkflowStep<IngestionStepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(IngestionStep.class);

    @Autowired
    private IngestionProgressService ingestionProgressService;

    @Resource(name = "ingestionSFTPProviderService")
    private IngestionProviderService ingestionSFTPProviderService;

    @Resource(name = "ingestionAPIProviderService")
    private IngestionProviderService ingestionAPIProviderService;

    @Resource(name = "ingestionSQLProviderService")
    private IngestionProviderService ingestionSQLProviderService;

    @Autowired
    protected JobService jobService;

    private IngestionProgress progress;

    @Override
    public void execute() {
        try {
            log.info("Entering IngestionStep execution");
            progress = getConfiguration().getIngestionProgress();
            HdfsPodContext.changeHdfsPodId(progress.getHdfsPod());
            Ingestion ingestion = getConfiguration().getIngestion();
            ProviderConfiguration providerConfiguration = getConfiguration().getProviderConfiguration();
            ingestion.setProviderConfiguration(providerConfiguration);
            progress.setIngestion(ingestion);
            switch (progress.getIngestion().getIngestionType()) {
            case SFTP:
                ingestionSFTPProviderService.ingest(progress);
                break;
            case API:
                ingestionAPIProviderService.ingest(progress);
                break;
            case SQL_TO_CSVGZ:
            case SQL_TO_SOURCE:
                ingestionSQLProviderService.ingest(progress);
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Ingestion type %s is not supported", ingestion.getIngestionType()));
            }
            log.info("Exiting IngestionStep execute()");
        } catch (Exception e) {
            failByException(e);
        }
    }

    private void failByException(Exception e) {
        progress = ingestionProgressService.updateProgress(progress).status(ProgressStatus.FAILED)
                .errorMessage(e.getMessage().substring(0, 1000)).commit(true);
        log.error("Ingestion failed for progress: " + progress.toString(), e);
    }
}
