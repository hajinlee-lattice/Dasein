package com.latticeengines.datacloudapi.engine.publication.service;

import java.util.List;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationRequest;

public interface PublicationService {

    List<PublicationProgress> scan();

    PublicationProgress kickoff(String publicationName, PublicationRequest request);

    AppSubmission publish(String publicationName, PublicationRequest request);

    ProgressStatus findProgressAtVersion(String publicationName, String version);

}
