package com.latticeengines.propdata.engine.publication.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.Publication;

public interface PublicationEntityMgr {

    Publication findByPublicationName(String publicationName);
    Publication addPublication(Publication publication);
    void removePublication(String publicationName);
    List<Publication> findAll();
}
