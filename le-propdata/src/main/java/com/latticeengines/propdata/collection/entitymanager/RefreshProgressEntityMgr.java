package com.latticeengines.propdata.collection.entitymanager;

import java.util.Date;

import com.latticeengines.domain.exposed.propdata.collection.RefreshProgress;
import com.latticeengines.propdata.core.source.ServingSource;

public interface RefreshProgressEntityMgr extends ProgressEntityMgr<RefreshProgress> {

    RefreshProgress insertNewProgress(ServingSource source, Date pivotDate, String creator);

    RefreshProgress findProgressByBaseVersion(ServingSource source, String baseVersion);

}
