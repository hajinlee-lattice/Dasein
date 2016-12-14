package com.latticeengines.admin.tenant.batonadapter.bardjams;

import com.latticeengines.admin.entitymgr.BardJamsEntityMgr;
import com.latticeengines.domain.exposed.admin.BardJamsTenant;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;

public class BardJamsDestroyer implements CustomerSpaceServiceDestroyer {

    private BardJamsEntityMgr bardJamsEntityMgr;

    @Override
    public boolean destroy(CustomerSpace space, String serviceName) {
        String tenantId = space.getTenantId();
        BardJamsTenant tenant = bardJamsEntityMgr.findByTenant(tenantId);
        if (tenant != null) {
            bardJamsEntityMgr.delete(tenant);
        }
        return true;
    }

    protected void setBardJamsEntityMgr(BardJamsEntityMgr bardJamsEntityMgr) {
        this.bardJamsEntityMgr = bardJamsEntityMgr;
    }
}
