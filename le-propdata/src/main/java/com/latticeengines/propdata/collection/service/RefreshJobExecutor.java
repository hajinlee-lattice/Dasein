package com.latticeengines.propdata.collection.service;

import com.latticeengines.domain.exposed.propdata.manage.Progress;

public interface RefreshJobExecutor {

    void kickOffNewProgress();

    void proceedProgress(Progress progress);

}
