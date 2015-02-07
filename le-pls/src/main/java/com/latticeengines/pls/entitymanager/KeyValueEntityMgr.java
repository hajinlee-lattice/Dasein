package com.latticeengines.pls.entitymanager;

import com.latticeengines.domain.exposed.pls.KeyValue;

public interface KeyValueEntityMgr {

    KeyValue getDataByRelatedEntityId(Long entityPid);

}
