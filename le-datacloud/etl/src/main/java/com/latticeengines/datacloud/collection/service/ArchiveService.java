package com.latticeengines.datacloud.collection.service;

import java.util.Date;

import com.latticeengines.datacloud.core.source.DataImportedFromDB;
import com.latticeengines.domain.exposed.datacloud.manage.ArchiveProgress;

public interface ArchiveService {

    ArchiveProgress startNewProgress(Date startDate, Date endDate, String creator);

    ArchiveProgress importFromDB(ArchiveProgress request);

    ArchiveProgress finish(ArchiveProgress progress);

    String getVersionString(ArchiveProgress progress);

    DataImportedFromDB getSource();

    String getBeanName();

}
