package com.latticeengines.propdata.collection.service;

import java.util.Date;

import com.latticeengines.domain.exposed.propdata.collection.RefreshProgress;
import com.latticeengines.propdata.collection.source.Source;

public interface RefreshService {

    RefreshProgress startNewProgress(Date pivotDate, String baseSourceVersion, String creator);

    RefreshProgress transform(RefreshProgress progress);

    RefreshProgress exportToDB(RefreshProgress progress);

    RefreshProgress finish(RefreshProgress progress);

    String getVersionString(RefreshProgress progress);

    String findBaseVersionForNewProgress();

    Source getSource();

}
