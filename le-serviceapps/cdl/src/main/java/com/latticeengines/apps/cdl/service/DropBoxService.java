package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.cdl.DropBox;

public interface DropBoxService {

    DropBox create();

    void delete();

    String getDropBoxBucket();
    String getDropBoxPrefix();

}
