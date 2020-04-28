package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.AttributeSetDao;
import com.latticeengines.apps.cdl.entitymgr.AttributeSetEntityMgr;
import com.latticeengines.apps.cdl.repository.AttributeSetRepository;
import com.latticeengines.apps.cdl.repository.reader.AttributeSetReaderRepository;
import com.latticeengines.apps.cdl.repository.writer.AttributeSetWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.AttributeSet;

@Component("attributeSetEntityMgr")
public class AttributeSetEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<AttributeSetRepository, AttributeSet, Long>
        implements AttributeSetEntityMgr {

    @Inject
    private AttributeSetEntityMgrImpl _self;

    @Inject
    private AttributeSetDao attributeSetDao;

    @Inject
    private AttributeSetReaderRepository attributeSetReaderRepository;

    @Inject
    private AttributeSetWriterRepository attributeSetWriterRepository;

    @Override
    protected AttributeSetRepository getReaderRepo() {
        return attributeSetReaderRepository;
    }

    @Override
    protected AttributeSetRepository getWriterRepo() {
        return attributeSetWriterRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<AttributeSetRepository, AttributeSet, Long> getSelf() {
        return _self;
    }

    @Override
    public BaseDao<AttributeSet> getDao() {
        return attributeSetDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public AttributeSet findByName(String name) {
        return attributeSetReaderRepository.findByName(name);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public AttributeSet createAttributeSet(AttributeSet attributeSet) {
        attributeSet.setTenant(MultiTenantContext.getTenant());
        attributeSet.setName(AttributeSet.generateNameStr());
        attributeSetDao.create(attributeSet);
        return attributeSet;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public AttributeSet updateAttributeSet(AttributeSet attributeSet) {
        AttributeSet existingAttributeSet = findByName(attributeSet.getName());
        if (existingAttributeSet != null) {
            cloneAttributeSet(existingAttributeSet, attributeSet);
            attributeSetDao.update(existingAttributeSet);
            return existingAttributeSet;
        } else {
            throw new RuntimeException("Attribute set does not exist.");
        }
    }

    private void cloneAttributeSet(AttributeSet existingAttributeSet, AttributeSet attributeSet) {
        existingAttributeSet.setDisplayName(attributeSet.getDisplayName());
        existingAttributeSet.setDescription(attributeSet.getDescription());
        existingAttributeSet.setAttributesMap(attributeSet.getAttributesMap());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteByName(String name) {
        AttributeSet attributeSet = attributeSetWriterRepository.findByName(name);
        if (attributeSet != null) {
            delete(attributeSet);
        }
    }

}
