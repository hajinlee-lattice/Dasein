package com.latticeengines.encryption.exposed.service;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.joda.time.DateTime;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.encryption.functionalframework.EncryptionTestNGBase;

public class DataEncryptionServiceImplTestNG extends EncryptionTestNGBase {

    @Test(groups = "functional")
    public void testEncrypt() {
        CustomerSpace space = CustomerSpace.parse("LETest" + DateTime.now().getMillis());
        CustomerSpace secondSpace = new CustomerSpace(space.getContractId(), "2", space.getSpaceId());

        try {
            assertFalse(dataEncryptionService.isEncrypted(space));

            Tenant tenant = createEncryptedTenant(space);
            assertTrue(dataEncryptionService.isEncrypted(space));

            // Encrypt this contract again but with a different space
            assertFalse(dataEncryptionService.isEncrypted(secondSpace));
            Tenant secondTenant = createEncryptedTenant(secondSpace);
            assertTrue(dataEncryptionService.isEncrypted(secondSpace));
        } finally {
            cleanup(space);
            cleanup(secondSpace);
        }
    }

}
