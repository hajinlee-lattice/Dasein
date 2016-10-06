package com.latticeengines.encryption.exposed.service;

import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.encryption.functionalframework.EncryptionTestNGBase;

public class EncryptedAvroTestNG extends EncryptionTestNGBase {

    @Test(groups = "functional")
    public void testReadAvroFromEncryptedLocation() throws Exception {
        CustomerSpace space = CustomerSpace.parse("LETest" + DateTime.now().getMillis());
        try {
            Tenant tenant = createEncryptedTenant(space);
            HdfsUtils.copyLocalResourceToHdfs(yarnConfiguration,
                    "com/latticeengines/encryption/exposed/service/test.avro",
                    "/user/s-analytics/customers/" + tenant.getId());
            AvroUtils.getData(yarnConfiguration, new Path("/user/s-analytics/customers/" + tenant.getId()
                    + "/test.avro"));
        } finally {
            cleanup(space);
        }
    }
}
