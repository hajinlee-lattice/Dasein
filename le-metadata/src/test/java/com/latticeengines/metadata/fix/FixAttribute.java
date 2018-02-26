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
        Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse("Halladay").toString());
        MultiTenantContext.setTenant(tenant);
        Table t = tableEntityMgr.findByName("copy_52e61053_d618_4b7c_8a0a_02dd805fffdb");
        Attribute a = t.getAttribute("State");
        System.out.println(a.getPid());
        a.setInterfaceName(InterfaceName.State);
        attributeEntityMgr.update(a);
    }
}
