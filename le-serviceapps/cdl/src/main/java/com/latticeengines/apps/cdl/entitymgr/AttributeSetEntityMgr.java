package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.metadata.AttributeSet;

public interface AttributeSetEntityMgr extends BaseEntityMgrRepository<AttributeSet, Long> {

    AttributeSet findByName(String name);

    AttributeSet createAttributeSet(AttributeSet attributeSet);

    AttributeSet updateAttributeSet(AttributeSet attributeSet);

    void deleteByName(String name);

    AttributeSet createAttributeSet(String name, AttributeSet attributeSet);

    AttributeSet createAttributeSet(Map<String, Set<String>> existingAttributesMap, AttributeSet attributeSet);

    AttributeSet createDefaultAttributeSet();

    List<AttributeSet> findAllWithAttributesMap();

    AttributeSet findByDisPlayName(String displayName);
}
