package com.latticeengines.datacloud.collection.service;

import java.util.Date;

import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.domain.exposed.datacloud.manage.RefreshProgress;

public interface RefreshService {

    RefreshProgress startNewProgress(Date pivotDate, String baseSourceVersion, String creator);

    RefreshProgress transform(RefreshProgress progress);

    RefreshProgress exportToDB(RefreshProgress progress);

    RefreshProgress finish(RefreshProgress progress);

    String getVersionString(RefreshProgress progress);

    String findBaseVersionForNewProgress();

    DerivedSource getSource();

    void purgeOldVersions();

    String getBeanName();

}
