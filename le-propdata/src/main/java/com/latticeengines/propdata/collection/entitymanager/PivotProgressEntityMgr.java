package com.latticeengines.propdata.collection.entitymanager;

import java.util.Date;

import com.latticeengines.domain.exposed.propdata.collection.PivotProgress;
import com.latticeengines.propdata.collection.source.PivotedSource;

public interface PivotProgressEntityMgr extends ProgressEntityMgr<PivotProgress> {

    PivotProgress insertNewProgress(PivotedSource source, Date pivotDate, String creator);

    PivotProgress findProgressByBaseVersion(PivotedSource source, String baseVersion);

}
