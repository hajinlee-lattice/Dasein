package com.latticeengines.admin.entitymgr;

import com.latticeengines.domain.exposed.admin.BardJamsTenant;

public interface BardJamsEntityMgr {

    void create(BardJamsTenant request);

    void delete(BardJamsTenant request);

    BardJamsTenant findByKey(BardJamsTenant request);

}
