package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
            updateExistingAttributeSet(existingAttributeSet, attributeSet);
            attributeSetDao.update(existingAttributeSet);
            return existingAttributeSet;
        } else {
            throw new RuntimeException(String.format("Attribute set %s does not exist.", attributeSet.getName()));
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public AttributeSet cloneAttributeSet(String name, AttributeSet attributeSet) {
        AttributeSet existingAttributeSet = findByName(name);
        attributeSet.setTenant(MultiTenantContext.getTenant());
        attributeSet.setName(AttributeSet.generateNameStr());
        if (existingAttributeSet != null) {
            mergeAttributesMap(existingAttributeSet.getAttributesMap(), attributeSet);
            attributeSet.setAttributesMap(existingAttributeSet.getAttributesMap());
            attributeSetDao.create(attributeSet);
            return attributeSet;
        } else {
            throw new RuntimeException(String.format("Attribute set %s does not exist.", attributeSet.getName()));
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public AttributeSet cloneAttributeSet(Map<String, Set<String>> existingAttributesMap, AttributeSet attributeSet) {
        attributeSet.setTenant(MultiTenantContext.getTenant());
        attributeSet.setName(AttributeSet.generateNameStr());
        mergeAttributesMap(existingAttributesMap, attributeSet);
        attributeSet.setAttributesMap(existingAttributesMap);
        attributeSetDao.create(attributeSet);
        return attributeSet;
    }

    private void mergeAttributesMap(Map<String, Set<String>> existingAttributeSetAttributesMap, AttributeSet overrideAttributeSet) {
        if (overrideAttributeSet.getAttributesMap() != null) {
            for (Map.Entry<String, Set<String>> entry : overrideAttributeSet.getAttributesMap().entrySet()) {
                existingAttributeSetAttributesMap.put(entry.getKey(), entry.getValue());
            }
        }
    }

    private void updateExistingAttributeSet(AttributeSet existingAttributeSet, AttributeSet attributeSet) {
        if (attributeSet.getDisplayName() != null) {
            existingAttributeSet.setDisplayName(attributeSet.getDisplayName());
        }
        if (attributeSet.getDescription() != null) {
            existingAttributeSet.setDescription(attributeSet.getDescription());
        }
        mergeAttributesMap(existingAttributeSet.getAttributesMap(), attributeSet);
        existingAttributeSet.setUpdatedBy(attributeSet.getUpdatedBy());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteByName(String name) {
        AttributeSet attributeSet = attributeSetWriterRepository.findByName(name);
        if (attributeSet != null) {
            delete(attributeSet);
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AttributeSet> findAll() {
        return getReadOrWriteRepository().findAll();
    }

}
