package com.latticeengines.encryption.functionalframework;

import static org.testng.Assert.assertTrue;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.encryption.exposed.service.DataEncryptionService;
import com.latticeengines.encryption.exposed.service.KeyManagementService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.testframework.security.impl.GlobalAuthFunctionalTestBed;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-encryption-context.xml" })
public class EncryptionTestNGBase extends AbstractTestNGSpringContextTests {
    @Autowired
    protected DataEncryptionService dataEncryptionService;

    @Autowired
    protected KeyManagementService keyManagementService;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected TenantService tenantService;

    private GlobalAuthFunctionalTestBed globalAuthFunctionalTestBed;

    @Value("${encryption.enabled}")
    protected boolean encryptionEnabled;

    @BeforeClass(groups = "functional")
    private void setup() {
        assertTrue(encryptionEnabled, "Encryption is not enabled (encryption.enabled is false)");
    }

    protected Tenant createEncryptedTenant(CustomerSpace space) {
        Tenant tenant = createTenant(space);
        dataEncryptionService.encrypt(space);
        return tenant;
    }

    protected void cleanup(CustomerSpace space) {
        List<String> paths = dataEncryptionService.getEncryptedPaths(space);
        for (String path : paths) {
            try {
                HdfsUtils.rmdir(yarnConfiguration, path);
            } catch (Exception e) {
                // pass
            }
        }

        keyManagementService.deleteKey(space);

        Tenant tenant = tenantService.findByTenantId(space.toString());
        if (tenant != null) {
            tenantService.discardTenant(tenant);
        }
    }

    private Tenant createTenant(CustomerSpace space) {
        Tenant tenant = new Tenant();
        tenant.setName(space.toString());
        tenant.setId(space.toString());
        tenantService.registerTenant(tenant);
        return tenant;
    }
}
