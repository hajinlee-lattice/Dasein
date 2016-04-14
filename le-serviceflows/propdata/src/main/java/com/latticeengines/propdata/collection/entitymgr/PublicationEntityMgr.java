package com.latticeengines.propdata.collection.entitymgr;

import com.latticeengines.domain.exposed.propdata.manage.Publication;

public interface PublicationEntityMgr {

    Publication findBySourceName(String sourceName);
    Publication addPublication(Publication publication);
    void removePublication(String publicationName);

}
