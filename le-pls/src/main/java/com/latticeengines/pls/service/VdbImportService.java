package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.pls.VdbGetLoadStatusConfig;
import com.latticeengines.domain.exposed.pls.VdbLoadTableCancel;
import com.latticeengines.domain.exposed.pls.VdbLoadTableConfig;
import com.latticeengines.domain.exposed.pls.VdbLoadTableStatus;

public interface VdbImportService {
    String submitLoadTableJob(VdbLoadTableConfig loadConfig);

    boolean cancelLoadTableJob(String applicationId, VdbLoadTableCancel cancelConfig);

    VdbLoadTableStatus getLoadTableStatus(VdbGetLoadStatusConfig config);
}
