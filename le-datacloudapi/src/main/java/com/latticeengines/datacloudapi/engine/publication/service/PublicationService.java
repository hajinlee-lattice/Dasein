package com.latticeengines.datacloudapi.engine.publication.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.orchestration.EngineProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationRequest;

public interface PublicationService {

    List<PublicationProgress> scan(String hdfsPod);

    PublicationProgress publish(String publicationName, PublicationRequest request, String hdfsPod);

    EngineProgress status(String publicationName, String version);

}
