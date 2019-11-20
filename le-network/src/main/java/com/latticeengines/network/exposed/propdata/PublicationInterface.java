package com.latticeengines.network.exposed.propdata;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationRequest;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationResponse;

public interface PublicationInterface {

    List<PublicationProgress> scan(String hdfsPod);

    PublicationResponse publish(String publicationName, PublicationRequest publicationRequest, String hdfsPod);

}
