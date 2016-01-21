package com.latticeengines.propdata.collection.entitymanager;

import java.util.Date;

import com.latticeengines.domain.exposed.propdata.manage.RefreshProgress;
import com.latticeengines.propdata.core.source.DerivedSource;

public interface RefreshProgressEntityMgr extends ProgressEntityMgr<RefreshProgress> {

    RefreshProgress insertNewProgress(DerivedSource source, Date pivotDate, String creator);

    RefreshProgress findProgressByBaseVersion(DerivedSource source, String baseVersion);

}
