package com.latticeengines.network.exposed.propdata;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;

public interface PublicationInterface {

    List<PublicationProgress> scan(String hdfsPod);

    PublicationProgress publish(String publicationName, String submitter, String hdfsPod);

}
