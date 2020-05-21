package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
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
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.metadata.AttributeSet;
import com.latticeengines.domain.exposed.util.AttributeUtils;

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
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public AttributeSet findByDisPlayName(String displayName) {
        return attributeSetReaderRepository.findByDisplayName(displayName);
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
    public AttributeSet createAttributeSet(String name, AttributeSet attributeSet) {
        AttributeSet existingAttributeSet = findByName(name);
        if (existingAttributeSet != null) {
            attributeSet.setTenant(MultiTenantContext.getTenant());
            attributeSet.setName(AttributeSet.generateNameStr());
            if (StringUtils.isEmpty(attributeSet.getDisplayName())) {
                attributeSet.setDisplayName(existingAttributeSet.getDisplayName());
            }
            if (StringUtils.isEmpty(attributeSet.getDisplayName())) {
                attributeSet.setDescription(existingAttributeSet.getDescription());
            }
            attributeSet.setAttributesMap(existingAttributeSet.getAttributesMap());
            attributeSetDao.create(attributeSet);
            return attributeSet;
        } else {
            throw new RuntimeException(String.format("Attribute set %s does not exist.", attributeSet.getName()));
        }
    }

    private AttributeSet getDefaultAttributeSet() {
        AttributeSet attributeSet = new AttributeSet();
        attributeSet.setName(AttributeUtils.DEFAULT_ATTRIBUTE_SET_NAME);
        attributeSet.setTenant(MultiTenantContext.getTenant());
        attributeSet.setDisplayName(AttributeUtils.DEFAULT_ATTRIBUTE_SET_DISPLAY_NAME);
        attributeSet.setDescription(AttributeUtils.DEFAULT_ATTRIBUTE_SET_DESCRIPTION);
        attributeSet.setCreatedBy(CDLConstants.DEFAULT_SYSTEM_USER);
        attributeSet.setUpdatedBy(CDLConstants.DEFAULT_SYSTEM_USER);
        return attributeSet;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public AttributeSet createDefaultAttributeSet() {
        AttributeSet defaultSet = getDefaultAttributeSet();
        attributeSetDao.create(defaultSet);
        return defaultSet;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public AttributeSet createAttributeSet(Map<String, Set<String>> existingAttributesMap, AttributeSet attributeSet) {
        attributeSet.setTenant(MultiTenantContext.getTenant());
        attributeSet.setName(AttributeSet.generateNameStr());
        mergeAttributesMap(existingAttributesMap, attributeSet);
        attributeSet.setAttributesMap(existingAttributesMap);
        attributeSetDao.create(attributeSet);
        return attributeSet;
    }

    private void mergeAttributesMap(Map<String, Set<String>> existingAttributeSetAttributesMap, AttributeSet overrideAttributeSet) {
        if (MapUtils.isNotEmpty(overrideAttributeSet.getAttributesMap())) {
            for (Map.Entry<String, Set<String>> entry : overrideAttributeSet.getAttributesMap().entrySet()) {
                existingAttributeSetAttributesMap.put(entry.getKey(), entry.getValue() == null ? new HashSet<>() : entry.getValue());
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
        Map<String, Set<String>> existingAttributeSetAttributesMap = existingAttributeSet.getAttributesMap();
        mergeAttributesMap(existingAttributeSetAttributesMap, attributeSet);
        existingAttributeSet.setAttributesMap(existingAttributeSetAttributesMap);
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
        List<Object[]> results = getReadOrWriteRepository().findAttributeSets();
        // ignore attribute map when query list
        return extractResultsToAttributeSets(results);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AttributeSet> findAllWithAttributesMap() {
        return getReadOrWriteRepository().findAll();
    }

    private List<AttributeSet> extractResultsToAttributeSets(List<Object[]> objects) {
        if (CollectionUtils.isEmpty(objects)) {
            return new ArrayList<>();
        }
        return objects.stream().map(object -> {
            AttributeSet attributeSet = new AttributeSet();
            attributeSet.setName((String) object[0]);
            attributeSet.setDisplayName((String) object[1]);
            attributeSet.setDescription((String) object[2]);
            attributeSet.setCreated((Date) object[3]);
            attributeSet.setUpdated((Date) object[4]);
            attributeSet.setCreatedBy((String) object[5]);
            attributeSet.setUpdatedBy((String) object[6]);
            return attributeSet;
        }).collect(Collectors.toList());

    }

}
