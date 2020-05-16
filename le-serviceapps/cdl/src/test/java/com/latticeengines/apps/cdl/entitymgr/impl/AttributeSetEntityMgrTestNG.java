package com.latticeengines.apps.cdl.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.springframework.retry.support.RetryTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.AttributeSetEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.RetryUtils;
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
        AttributeSet attributeSet = createAttributeSet(displayName, accountAttributes, contactAttributes);
        attributeSetEntityMgr.createAttributeSet(attributeSet);
        attributeSet = attributeSetEntityMgr.findByName(attributeSet.getName());
        String attributeSetName = attributeSet.getName();
        verifyAttributeSet(attributeSet, attributeSet.getName(), displayName, accountAttributes, contactAttributes);
        attributeSet.setDisplayName(displayName2);
        Set<String> emptySet = new HashSet<>();
        updateContactAttributes(attributeSet, emptySet);
        attributeSetEntityMgr.updateAttributeSet(attributeSet);
        RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AssertionError.class), null);
        retry.execute(context -> {
            AttributeSet attributeSet2 = attributeSetEntityMgr.findByName(attributeSetName);
            verifyAttributeSet(attributeSet2, attributeSet2.getName(), displayName2, accountAttributes, emptySet);
            return true;
        });
        assertEquals(attributeSetEntityMgr.findAll().size(), 1);

        attributeSet = new AttributeSet();
        attributeSet.setDisplayName(displayName);
        attributeSet = attributeSetEntityMgr.createAttributeSet(attributeSetName, attributeSet);
        attributeSet = attributeSetEntityMgr.findByName(attributeSet.getName());
        verifyAttributeSet(attributeSet, attributeSet.getName(), displayName, accountAttributes, emptySet);

        attributeSetEntityMgr.deleteByName(attributeSetName);
        retry.execute(context -> {
            assertEquals(attributeSetEntityMgr.findAll().size(), 1);
            return true;
        });
    }

    private void updateContactAttributes(AttributeSet attributeSet, Set<String> set) {
        Map<String, Set<String>> attributesMap = attributeSet.getAttributesMap();
        attributesMap.put(Category.CONTACT_ATTRIBUTES.name(), set);
        attributeSet.setAttributesMap(attributesMap);
    }

    private AttributeSet createAttributeSet(String displayName, Set<String> accountAttributes, Set<String> contactAttributes) {
        AttributeSet attributeSet = new AttributeSet();
        attributeSet.setDisplayName(displayName);
        Map<String, Set<String>> attributesMap = new HashMap<>();
        if (accountAttributes != null) {
            attributesMap.put(Category.ACCOUNT_ATTRIBUTES.name(), accountAttributes);
        }
        if (contactAttributes != null) {
            attributesMap.put(Category.CONTACT_ATTRIBUTES.name(), contactAttributes);
        }
        attributeSet.setAttributesMap(attributesMap);
        return attributeSet;
    }

    private void verifyAttributeSet(AttributeSet attributeSet, String name, String displayName,
                                    Set<String> accountAttributes, Set<String> contactAttributes) {
        assertEquals(attributeSet.getName(), name);
        assertEquals(attributeSet.getDisplayName(), displayName);
        assertNotNull(attributeSet.getAttributes());
        assertNotNull(attributeSet.getCreated());
        assertNotNull(attributeSet.getUpdated());
        assertEquals(attributeSet.getAttributesMap().get(Category.ACCOUNT_ATTRIBUTES.name()).size(), accountAttributes.size());
        assertEquals(attributeSet.getAttributesMap().get(Category.CONTACT_ATTRIBUTES.name()).size(), contactAttributes.size());
    }
}
