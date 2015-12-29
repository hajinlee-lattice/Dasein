package com.latticeengines.propdata.collection.service;

import com.latticeengines.domain.exposed.propdata.collection.Progress;

public interface RefreshJobExecutor {

    void proceedProgress(Progress progress);

    void print();

}
