package com.latticeengines.propdata.collection.service;

import com.latticeengines.domain.exposed.propdata.collection.Progress;

public interface RefreshJobExecutor {

    void kickOffNewProgress();

    void proceedProgress(Progress progress);

}
