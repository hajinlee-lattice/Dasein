package com.latticeengines.datacloud.collection.service;

import com.latticeengines.domain.exposed.datacloud.manage.Progress;

public interface RefreshJobExecutor {

    void kickOffNewProgress();

    void proceedProgress(Progress progress);

    void purgeOldVersions();

}
