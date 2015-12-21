package com.latticeengines.propdata.collection.service;

import java.util.Date;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.collection.PivotProgress;

public interface PivotService {

    PivotProgress startNewProgress(Date pivotDate, String baseSourceVersion, String creator);

    PivotProgress startNewProgressIfOutDated(String creator);

    PivotProgress pivot(PivotProgress progress);

    PivotProgress exportToDB(PivotProgress progress);

    PivotProgress finish(PivotProgress progress);

    ArchiveProgress findRunningJobOnBaseSource();

    PivotProgress findJobToRetry();

    PivotProgress findRunningJob();

}
