package com.latticeengines.apps.cdl.entitymgr;

import java.util.Map;
import java.util.Set;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.metadata.AttributeSet;

public interface AttributeSetEntityMgr extends BaseEntityMgrRepository<AttributeSet, Long> {

    AttributeSet findByName(String name);

    AttributeSet createAttributeSet(AttributeSet attributeSet);

    AttributeSet updateAttributeSet(AttributeSet attributeSet);

    void deleteByName(String name);

    AttributeSet cloneAttributeSet(String name, AttributeSet attributeSet);

    AttributeSet cloneAttributeSet(Map<String, Set<String>> existingAttributesMap, AttributeSet attributeSet);
}
