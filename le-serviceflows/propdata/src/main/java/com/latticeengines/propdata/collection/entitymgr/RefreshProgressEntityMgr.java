package com.latticeengines.propdata.collection.entitymgr;

import java.util.Date;

import com.latticeengines.domain.exposed.datacloud.manage.RefreshProgress;
import com.latticeengines.propdata.core.source.DerivedSource;

public interface RefreshProgressEntityMgr extends ProgressEntityMgr<RefreshProgress> {

    RefreshProgress insertNewProgress(DerivedSource source, Date pivotDate, String creator);

    RefreshProgress findProgressByBaseVersion(DerivedSource source, String baseVersion);

}
