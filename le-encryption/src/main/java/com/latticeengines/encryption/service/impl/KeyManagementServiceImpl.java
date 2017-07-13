package com.latticeengines.encryption.service.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.encryption.EncryptionGlobalState;
import com.latticeengines.domain.exposed.encryption.KeyPolicy;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.encryption.exposed.service.KeyManagementService;

@Component("keyManagementService")
public class KeyManagementServiceImpl implements KeyManagementService {
    private static final Logger log = LoggerFactory.getLogger(DataEncryptionServiceImpl.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${encryption.key.policy}")
    private String keyPolicy;

    @Value("${encryption.key.policy.master.key.name}")
    private String masterKeyName;

    public String getKeyName(CustomerSpace space) {
        if (keyPolicy == null || keyPolicy.equals(KeyPolicy.MASTER)) {
            return masterKeyName;
        } else if (keyPolicy.equals(KeyPolicy.CONTRACT)) {
            return space.getContractId().toLowerCase();
        }

        throw new RuntimeException(String.format("Unsupported key policy %s", keyPolicy));
    }

    public void createKey(CustomerSpace space) {
        if (!EncryptionGlobalState.isEnabled()) {
            return;
        }

        log.info(String.format("Creating key for %s", space));
        if (keyPolicy == null || keyPolicy.equals(KeyPolicy.MASTER)) {
            return;
        }

        String name = getKeyName(space);
        log.info(String.format("Creating key %s for %s", name, space));
        if (HdfsUtils.keyExists(yarnConfiguration, name)) {
            log.warn(String.format("Key %s already exists", name));
            return;
        }

        try {
            HdfsUtils.createKey(yarnConfiguration, name);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_34001, e, new Object[] { space });
        }
    }

    @Override
    public void deleteKey(CustomerSpace space) {
        if (!EncryptionGlobalState.isEnabled()) {
            return;
        }

        if (keyPolicy == null || keyPolicy.equals(KeyPolicy.MASTER)) {
            return;
        }
        try {
            if (TenantLifecycleManager.getAll(space.getContractId()).size() > 1) {
                log.info(String.format(
                        "Not deleting key for customer %s because there are other tenants with contract %s", space,
                        space.getContractId()));
            }
        } catch (KeeperException.NoNodeException e) {
            // pass
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        String name = getKeyName(space);
        if (System.getProperty("LE_ENCRYPTION_KEEPKEY") != null) {
            log.info(String.format("Keeping key %s for %s", name, space));
            return;
        }

        log.info(String.format("Deleting key %s for %s", name, space));

        try {
            if (HdfsUtils.keyExists(yarnConfiguration, name)) {
                HdfsUtils.deleteKey(yarnConfiguration, name);
            } else {
                log.info(String.format("Not deleting because key doesn't exist for %s with name %s", space, name));
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_34001, e, new Object[] { space });
        }
    }
}
