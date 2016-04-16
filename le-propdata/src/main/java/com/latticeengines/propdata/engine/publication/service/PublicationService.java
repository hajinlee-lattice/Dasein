package com.latticeengines.propdata.engine.publication.service;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.domain.exposed.propdata.publication.PublicationRequest;

public interface PublicationService {

    List<PublicationProgress> scan(String hdfsPod);

    PublicationProgress publish(String publicationName, PublicationRequest request, String hdfsPod);

}
