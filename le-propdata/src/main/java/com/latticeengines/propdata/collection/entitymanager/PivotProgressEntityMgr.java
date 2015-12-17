package com.latticeengines.propdata.collection.entitymanager;

import java.util.Date;

import com.latticeengines.domain.exposed.propdata.collection.PivotProgress;
import com.latticeengines.propdata.collection.source.Source;

public interface PivotProgressEntityMgr extends ProgressEntityMgr<PivotProgress> {

    PivotProgress insertNewProgress(Source source, Date pivotDate, String creator);

    PivotProgress findProgressNotInFinalState(Source source);

}
