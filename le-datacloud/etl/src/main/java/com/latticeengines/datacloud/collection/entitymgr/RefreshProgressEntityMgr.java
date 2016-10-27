package com.latticeengines.datacloud.collection.entitymgr;

import java.util.Date;

import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.domain.exposed.datacloud.manage.RefreshProgress;

public interface RefreshProgressEntityMgr extends ProgressEntityMgr<RefreshProgress> {

    RefreshProgress insertNewProgress(DerivedSource source, Date pivotDate, String creator);

    RefreshProgress findProgressByBaseVersion(DerivedSource source, String baseVersion);

}
