package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.S3ImportSystemEntityMgr;
import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

@Component("s3ImportSystemService")
public class S3ImportSystemServiceImpl implements S3ImportSystemService {

    private static final Logger log = LoggerFactory.getLogger(S3ImportSystemServiceImpl.class);

    private static final String DEFAULTSYSTEM = "DefaultSystem";

    @Inject
    private S3ImportSystemEntityMgr s3ImportSystemEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public void createS3ImportSystem(String customerSpace, S3ImportSystem importSystem) {
        if (importSystem == null) {
            log.warn("Create NULL S3ImportSystem!");
            return;
        }
        if (s3ImportSystemEntityMgr.findS3ImportSystem(importSystem.getName()) != null) {
            throw new RuntimeException("Already have import system with name: " + importSystem.getName());
        }
        List<S3ImportSystem> currentSystems = s3ImportSystemEntityMgr.findAll();
        if (CollectionUtils.isEmpty(currentSystems)) {
            importSystem.setPriority(1);
            importSystem.setAccountSystemId(InterfaceName.CustomerAccountId.name());
        } else {
            if (importSystem.getPriority() == 1) {
                for (S3ImportSystem system : currentSystems) {
                    system.setPriority(system.getPriority() + 1);
                    s3ImportSystemEntityMgr.update(system);
                }
                importSystem.setAccountSystemId(InterfaceName.CustomerAccountId.name());
            } else {
                importSystem.setPriority(currentSystems.size() + 1);
            }
        }
        s3ImportSystemEntityMgr.createS3ImportSystem(importSystem);
    }

    @Override
    public void createDefaultImportSystem(String customerSpace) {
        S3ImportSystem importSystem = new S3ImportSystem();
        importSystem.setPriority(1);
        importSystem.setAccountSystemId(InterfaceName.CustomerAccountId.name());
        importSystem.setName(DEFAULTSYSTEM);
        importSystem.setDisplayName(DEFAULTSYSTEM);
        importSystem.setSystemType(S3ImportSystem.SystemType.Other);
        importSystem.setTenant(tenantEntityMgr.findByTenantId(CustomerSpace.parse(customerSpace).toString()));
        createS3ImportSystem(customerSpace, importSystem);
    }

    @Override
    public void updateS3ImportSystem(String customerSpace, S3ImportSystem importSystem) {
        S3ImportSystem s3ImportSystem = s3ImportSystemEntityMgr.findS3ImportSystem(importSystem.getName());
        if (s3ImportSystem == null) {
            log.warn("Cannot find import System with name: " + importSystem.getName());
            return;
        }
        s3ImportSystem.setDisplayName(importSystem.getDisplayName());
        if (StringUtils.isEmpty(s3ImportSystem.getAccountSystemId())) {
            s3ImportSystem.setAccountSystemId(importSystem.getAccountSystemId());
        }
        if (StringUtils.isEmpty(s3ImportSystem.getContactSystemId())) {
            s3ImportSystem.setContactSystemId(importSystem.getContactSystemId());
        }
        if (StringUtils.isEmpty(s3ImportSystem.getProductSystemId())) {
            s3ImportSystem.setProductSystemId(importSystem.getProductSystemId());
        }
        if (importSystem.getPriority() != s3ImportSystem.getPriority() && importSystem.getPriority() < Integer.MAX_VALUE) {
            int currentPriority = s3ImportSystem.getPriority();
            int destPriority = importSystem.getPriority();
            List<S3ImportSystem> currentSystems = s3ImportSystemEntityMgr.findAll();
            // 5->3
            if (currentPriority > destPriority) {
                for (S3ImportSystem system : currentSystems) {
                    if (system.getPriority() >= destPriority && system.getPriority() < currentPriority) {
                        system.setPriority(system.getPriority() + 1);
                        s3ImportSystemEntityMgr.update(system);
                    }
                }
            } else { // 3->5
                for (S3ImportSystem system : currentSystems) {
                    if (system.getPriority() <= destPriority && system.getPriority() > currentPriority) {
                        system.setPriority(system.getPriority() - 1);
                        s3ImportSystemEntityMgr.update(system);
                    }
                }
            }
            s3ImportSystem.setPriority(importSystem.getPriority());
        }
        s3ImportSystemEntityMgr.update(s3ImportSystem);
    }

    @Override
    public S3ImportSystem getS3ImportSystem(String customerSpace, String name) {
        S3ImportSystem importSystem = s3ImportSystemEntityMgr.findS3ImportSystem(name);
        if (importSystem == null && DEFAULTSYSTEM.equals(name)) {
            createDefaultImportSystem(customerSpace);
            importSystem = s3ImportSystemEntityMgr.findS3ImportSystem(name);
        }
        return importSystem;
    }

    @Override
    public List<S3ImportSystem> getAllS3ImportSystem(String customerSpace) {
        return s3ImportSystemEntityMgr.findAll();
    }
}
