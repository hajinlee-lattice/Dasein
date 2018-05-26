package com.latticeengines.proxy.exposed.lp;

import com.latticeengines.domain.exposed.metadata.Table;

public interface ModelCopyProxy {

    String copyModel(String sourceTenant, String targetTenant, String modelGuid);

    Table cloneTrainingTable(String customerSpace, String modelGuid);

}
