package com.latticeengines.propdata.collection.entitymanager;

import java.util.Date;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.propdata.collection.source.Source;

public interface ArchiveProgressEntityMgr extends ProgressEntityMgr<ArchiveProgress> {

    ArchiveProgress insertNewProgress(Source source, Date startDate, Date endDate, String creator);

}
