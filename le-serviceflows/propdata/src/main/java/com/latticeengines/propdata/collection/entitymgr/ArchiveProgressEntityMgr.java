package com.latticeengines.propdata.collection.entitymgr;

import java.util.Date;

import com.latticeengines.domain.exposed.propdata.manage.ArchiveProgress;
import com.latticeengines.propdata.core.source.Source;

public interface ArchiveProgressEntityMgr extends ProgressEntityMgr<ArchiveProgress> {

    ArchiveProgress insertNewProgress(Source source, Date startDate, Date endDate, String creator);

}
