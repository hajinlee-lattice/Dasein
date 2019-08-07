package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreDetail;
import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreInfo;

public interface ConvertBatchStoreInfoService {

    ConvertBatchStoreInfo create(String customerSpace);

    ConvertBatchStoreInfo getByPid(String customerSpace, Long pid);

    void updateDetails(String customerSpace, Long pid, ConvertBatchStoreDetail convertDetail);
}
