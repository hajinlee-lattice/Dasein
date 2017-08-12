package com.latticeengines.network.exposed.propdata;

import java.util.List;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationRequest;

public interface PublicationInterface {

    List<PublicationProgress> scan(String hdfsPod);

    AppSubmission publish(String publicationName, PublicationRequest publicationRequest, String hdfsPod);

}
