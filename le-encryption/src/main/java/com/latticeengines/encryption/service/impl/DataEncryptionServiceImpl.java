package com.latticeengines.encryption.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.config.ConfigurationController;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceScope;
import com.latticeengines.domain.exposed.encryption.EncryptionGlobalState;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.encryption.exposed.service.DataEncryptionService;
import com.latticeengines.encryption.exposed.service.KeyManagementService;

@Component("dataEncryptionService")
public class DataEncryptionServiceImpl implements DataEncryptionService {

    private static final Logger log = LoggerFactory.getLogger(DataEncryptionServiceImpl.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private KeyManagementService keyManagementService;

    @Override
    public boolean isEncrypted(CustomerSpace space) {
        try {
            ConfigurationController<CustomerSpaceScope> controller = ConfigurationController
                    .construct(new CustomerSpaceScope(space));
            return controller.exists(new Path("/EncryptionKey"));
        } catch (Exception e) {
            log.error(String.format("Error determining whether customer %s is encrypted.", space), e);
            return false;
        }
    }

    @Override
    public void encrypt(CustomerSpace space) {
        log.info(String.format("Setting up customer %s's data as encrypted", space));
        try {
            if (!EncryptionGlobalState.isEnabled()) {
                log.info("Encryption is not enabled");
                return;
            }

            if (isEncrypted(space)) {
                log.warn(String.format("Customer %s is already encrypted", space));
                return;
            }

            String keyName = keyManagementService.getKeyName(space);
            keyManagementService.createKey(space);

            List<String> paths = getEncryptedPaths(space);
            ListIterator<String> iter = paths.listIterator();
            while (iter.hasNext()) {
                String path = iter.next();
                if (HdfsUtils.isEncryptionZone(yarnConfiguration, path)) {
                    log.info(String.format("%s is already encrypted", path));
                    iter.remove();
                    continue;
                }

                if (HdfsUtils.fileExists(yarnConfiguration, path)) {
                    int numfiles = HdfsUtils.getFilesForDir(yarnConfiguration, path).size();
                    if (numfiles == 0) {
                        HdfsUtils.rmdir(yarnConfiguration, path);
                    } else {
                        throw new LedpException(LedpCode.LEDP_34000, new String[] { path });
                    }
                }
            }

            for (String path : paths) {
                HdfsUtils.mkdir(yarnConfiguration, path);
                HdfsUtils.createEncryptionZone(yarnConfiguration, path, keyName);
            }

            ConfigurationController<CustomerSpaceScope> controller = ConfigurationController
                    .construct(new CustomerSpaceScope(space));
            controller.upsert(new Path("/EncryptionKey"), new Document(keyName));
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to encrypt customer %s", space.toString()), e);
        }
    }

    @Override
    public List<String> getEncryptedPaths(CustomerSpace space) {
        List<String> paths = new ArrayList<>();
        String spacePath = PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), space.getContractId())
                .toString();
        String modelPath = "/user/s-analytics/customers/" + space.toString();
        paths.add(spacePath);
        paths.add(modelPath);
        return paths;
    }

    @Override
    public void deleteKey(CustomerSpace space) {
        log.info(String.format("Deleting customer %s's key", space));
        keyManagementService.deleteKey(space);
    }
}
