package com.latticeengines.datacloudapi.qbean;

import java.util.concurrent.Callable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloudapi.engine.ingestion.service.IngestionService;
import com.latticeengines.datacloudapi.engine.orchestration.service.OrchestrationService;
import com.latticeengines.datacloudapi.engine.publication.service.PublicationService;
import com.latticeengines.datacloudapi.engine.transformation.service.SourceTransformationService;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Component("dataCloudRefresh")
public class DataCloudRefreshBean implements QuartzJobBean {

    @Autowired
    private SourceTransformationService transformationService;

    @Autowired
    private PublicationService publicationService;

    @Autowired
    private IngestionService ingestionService;

    @Autowired
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
