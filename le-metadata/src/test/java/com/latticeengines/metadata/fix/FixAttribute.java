package com.latticeengines.metadata.fix;

import javax.inject.Inject;

import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.entitymgr.AttributeEntityMgr;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;

public class FixAttribute extends MetadataFunctionalTestNGBase {

    @Inject
    private AttributeEntityMgr attributeEntityMgr;

    @Test(groups = "manual")
    public void test() {
        Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse("SmartBearSoftware").toString());
        MultiTenantContext.setTenant(tenant);
        Table t = tableEntityMgr.findByName("copy_68d4c341_c94e_473c_bd82_3de26b6c6e9f");
        Attribute a = t.getAttribute("State");
        System.out.println(a.getPid());
        a.setInterfaceName(InterfaceName.State);
        attributeEntityMgr.update(a);
    }
}
