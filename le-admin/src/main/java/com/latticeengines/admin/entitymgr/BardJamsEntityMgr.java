package com.latticeengines.admin.entitymgr;

import com.latticeengines.domain.exposed.admin.BardJamsTenants;

public interface BardJamsEntityMgr {

    void create(BardJamsTenants request);

    void delete(BardJamsTenants request);

    BardJamsTenants findByKey(BardJamsTenants request);

}
