package com.latticeengines.metadata.hive.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.hive.HiveTableDao;

public class HiveTableDaoImplTestNG extends MetadataFunctionalTestNGBase {

    @Autowired
    private HiveTableDao hiveTableDao;

    private Tenant tenant;

    @BeforeClass(groups = "manual")
    public void setup() {
        super.setup();
        tenant = tenantEntityMgr.findByTenantId(customerSpace1);
        MultiTenantContext.setTenant(tenant);
    }

    @Test(groups = "manual")
    public void test() {

        Table t = new Table();
        t.setName("test_table");

        Extract extract = new Extract();
        extract.setPath(
                "/user/s-analytics/customers/T1.T1.Production/data/DataCloudMatchEvent_F7411AB3-2596-4D9A-98C0-D0BA70087C42_with_std_attrib/samples/allTraining-r-00000.avro");
        t.addExtract(extract);
        hiveTableDao.create(t);
    }
}
