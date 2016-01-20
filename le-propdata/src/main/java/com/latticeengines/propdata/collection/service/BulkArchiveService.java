package com.latticeengines.propdata.collection.service;

import com.latticeengines.domain.exposed.propdata.manage.ArchiveProgress;

public interface BulkArchiveService extends ArchiveService {

    ArchiveProgress startNewProgress(String creator);

}
