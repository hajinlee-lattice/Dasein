package com.latticeengines.propdata.collection.service;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;

public interface PublicationService {

    List<PublicationProgress> scan(String hdfsPod);

    PublicationProgress publish(String publicationName, String creator, String hdfsPod);

}
