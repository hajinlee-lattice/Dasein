package com.latticeengines.apps.cdl.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.AttributeSetEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.metadata.AttributeSet;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({SimpleRetryListener.class})
public class AttributeSetEntityMgrTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private AttributeSetEntityMgr attributeSetEntityMgr;

    private Set<String> accountAttributes = SchemaRepository.getDefaultExportAttributes(BusinessEntity.Account, true)
            .stream().map(InterfaceName::name).collect(Collectors.toSet());
    private Set<String> contactAttributes = SchemaRepository.getDefaultExportAttributes(BusinessEntity.Contact, true)
            .stream().map(InterfaceName::name).collect(Collectors.toSet());

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testCrud() {
        String displayName = "TestAttributeSet";
        String displayName2 = "TestAttributeSet2";
        AttributeSet attributeSet = createAttributeSet(displayName);
        attributeSetEntityMgr.createAttributeSet(attributeSet);
        attributeSet = attributeSetEntityMgr.findByName(attributeSet.getName());
        verifyAttributeSet(attributeSet, attributeSet.getName(), displayName, accountAttributes, contactAttributes);
        attributeSet.setDisplayName(displayName2);
        Map<String, Set<String>> attributesMap = attributeSet.getAttributesMap();
        attributesMap.put(Category.CONTACT_ATTRIBUTES.name(), new HashSet<>());
        attributeSet.setAttributesMap(attributesMap);
        attributeSetEntityMgr.updateAttributeSet(attributeSet);
        attributeSet = attributeSetEntityMgr.findByName(attributeSet.getName());
        verifyAttributeSet(attributeSet, attributeSet.getName(), displayName2, accountAttributes, new HashSet<>());
        assertEquals(attributeSetEntityMgr.findAll().size(), 1);
        attributeSetEntityMgr.deleteByName(attributeSet.getName());
        assertEquals(attributeSetEntityMgr.findAll().size(), 0);
    }

    private AttributeSet createAttributeSet(String displayName) {
        AttributeSet attributeSet = new AttributeSet();
        attributeSet.setDisplayName(displayName);
        Map<String, Set<String>> attributesMap = new HashMap<>();
        attributesMap.put(Category.ACCOUNT_ATTRIBUTES.name(), accountAttributes);
        attributesMap.put(Category.CONTACT_ATTRIBUTES.name(), contactAttributes);
        attributeSet.setAttributesMap(attributesMap);
        return attributeSet;
    }

    private void verifyAttributeSet(AttributeSet attributeSet, String name, String displayName,
                                    Set<String> accountAttributes, Set<String> contactAttributes) {
        assertEquals(attributeSet.getDisplayName(), displayName);
        assertEquals(attributeSet.getName(), name);
        assertNotNull(attributeSet.getAttributes());
        assertNotNull(attributeSet.getCreated());
        assertNotNull(attributeSet.getUpdated());
        assertEquals(attributeSet.getAttributesMap().get(Category.ACCOUNT_ATTRIBUTES.name()).size(), accountAttributes.size());
        assertEquals(attributeSet.getAttributesMap().get(Category.CONTACT_ATTRIBUTES.name()).size(), contactAttributes.size());
    }
}
