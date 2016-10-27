package com.latticeengines.datacloud.collection.entitymgr;

import java.util.Date;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.domain.exposed.datacloud.manage.ArchiveProgress;

public interface ArchiveProgressEntityMgr extends ProgressEntityMgr<ArchiveProgress> {

    ArchiveProgress insertNewProgress(Source source, Date startDate, Date endDate, String creator);

}
