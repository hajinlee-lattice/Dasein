package com.latticeengines.datacloudapi.engine.publication.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationRequest;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationResponse;

public interface PublicationService {

    List<PublicationProgress> scan();

    PublicationProgress kickoff(String publicationName, PublicationRequest request);

    PublicationResponse publish(String publicationName, PublicationRequest request);

    ProgressStatus findProgressAtVersion(String publicationName, String version);

}
