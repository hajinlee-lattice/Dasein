package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.DropBoxDao;
import com.latticeengines.apps.cdl.entitymgr.DropBoxEntityMgr;
import com.latticeengines.apps.cdl.repository.DropBoxRepository;
import com.latticeengines.apps.cdl.repository.reader.DropBoxReaderRepository;
import com.latticeengines.apps.cdl.repository.writer.DropBoxWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DropBox;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("dropBoxEntityMgr")
public class DropBoxEntityMgrImpl //
        extends BaseReadWriteRepoEntityMgrImpl<DropBoxRepository, DropBox, Long> //
        implements DropBoxEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(DropBoxEntityMgrImpl.class);

    @Inject
    private DropBoxEntityMgrImpl _self;

    @Inject
    private DropBoxDao dao;

    @Inject
    private DropBoxReaderRepository readerRepository;

    @Inject
    private DropBoxWriterRepository writerRepository;

    @Override
    public BaseDao<DropBox> getDao() {
        return dao;
    }

    @Override
    protected DropBoxRepository getReaderRepo() {
        return readerRepository;
    }

    @Override
    protected DropBoxRepository getWriterRepo() {
        return writerRepository;
    }

    @Override
    protected DropBoxEntityMgrImpl getSelf() {
        return _self;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public DropBox createDropBox(String region) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (writerRepository.existsByTenant(tenant)) {
            log.warn("Tenant " + CustomerSpace.parse(tenant.getId()).getTenantId()
                    + " already has a dropbox, return the existing one instead of creating a new one.");
            return writerRepository.findByTenant(tenant);
        } else {
            DropBox dropbox = new DropBox();
            dropbox.setTenant(tenant);
            dropbox.setRegion(region);
            dropbox.setDropBox(findAvailableRandomStr());
            dao.create(dropbox);
            return dropbox;
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public DropBox createDropBox(Tenant tenant, String region) {
        if (writerRepository.existsByTenant(tenant)) {
            log.warn("Tenant " + CustomerSpace.parse(tenant.getId()).getTenantId()
                    + " already has a dropbox, return the existing one instead of creating a new one.");
            return writerRepository.findByTenant(tenant);
        } else {
            DropBox dropbox = new DropBox();
            dropbox.setTenant(tenant);
            dropbox.setRegion(region);
            dropbox.setDropBox(findAvailableRandomStr());
            dao.create(dropbox);
            return dropbox;
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DropBox getDropBox() {
        Tenant tenant = MultiTenantContext.getTenant();
        if (isReaderConnection()) {
            return getDropBoxFromReader(tenant);
        } else {
            return writerRepository.findByTenant(tenant);
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DropBox getDropBox(Tenant tenant) {
        if (isReaderConnection()) {
            return getDropBoxFromReader(tenant);
        } else {
            return writerRepository.findByTenant(tenant);
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Tenant getDropBoxOwner(String dropBox) {
        Tenant tenant;
        if (isReaderConnection()) {
            tenant = getDropBoxOwnerFromReader(dropBox);
        } else {
            tenant = writerRepository.findTenantByDropBox(dropBox);
        }
        return tenant;
    }

    @Transactional(transactionManager = "transactionManagerReader", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DropBox getDropBoxFromReader(Tenant tenant) {
        return readerRepository.findByTenant(tenant);
    }

    @Transactional(transactionManager = "transactionManagerReader", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Tenant getDropBoxOwnerFromReader(String dropBox) {
        return readerRepository.findTenantByDropBox(dropBox);
    }

    private String findAvailableRandomStr() {
        String str = RandomStringUtils.randomAlphanumeric(8).toLowerCase();
        while (writerRepository.existsByDropBox(str)) {
            str = RandomStringUtils.randomAlphanumeric(8).toLowerCase();
        }
        return str;
    }

}
