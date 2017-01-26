package com.latticeengines.datacloud.qbean;

import java.util.concurrent.Callable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.engine.ingestion.service.IngestionService;
import com.latticeengines.datacloud.engine.publication.service.PublicationService;
import com.latticeengines.datacloud.engine.transformation.service.SourceTransformationService;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Component("dataCloudRefresh")
public class DataCloudRefreshBean implements QuartzJobBean {

    @Autowired
    private SourceTransformationService transformationService;

    @Autowired
    private PublicationService publicationService;

    @Autowired
    private IngestionService ingestionService;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        return new DataCloudRefreshCallable.Builder() //
                .transformationProxy(transformationService) //
                .publicationProxy(publicationService) //
                .ingestionProxy(ingestionService).build();
    }

}
