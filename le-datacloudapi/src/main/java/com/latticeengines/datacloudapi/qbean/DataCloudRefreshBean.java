package com.latticeengines.datacloudapi.qbean;

import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloudapi.engine.ingestion.service.IngestionService;
import com.latticeengines.datacloudapi.engine.orchestration.service.OrchestrationService;
import com.latticeengines.datacloudapi.engine.publication.service.PublicationService;
import com.latticeengines.datacloudapi.engine.transformation.service.SourceTransformationService;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Component("dataCloudRefresh")
public class DataCloudRefreshBean implements QuartzJobBean {

    @Inject
    private SourceTransformationService transformationService;

    @Inject
    private PublicationService publicationService;

    @Inject
    private IngestionService ingestionService;

    @Inject
    private OrchestrationService orchestrationService;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        return new DataCloudRefreshCallable.Builder() //
                .transformationService(transformationService) //
                .publicationService(publicationService) //
                .ingestionService(ingestionService) //
                .orchestrationService(orchestrationService).build();
    }

}
