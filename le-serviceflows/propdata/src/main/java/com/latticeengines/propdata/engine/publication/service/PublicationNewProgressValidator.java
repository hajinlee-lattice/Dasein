package com.latticeengines.propdata.engine.publication.service;

import com.latticeengines.domain.exposed.datacloud.manage.Publication;

public interface PublicationNewProgressValidator {

    Boolean isValidToStartNewProgress(Publication publication, String currentVersion);

}
