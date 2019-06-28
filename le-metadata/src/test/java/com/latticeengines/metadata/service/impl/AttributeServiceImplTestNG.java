package com.latticeengines.metadata.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.data.domain.Pageable;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.metadata.entitymgr.impl.AttributeEntityMgrImplTestNG;
import com.latticeengines.metadata.service.AttributeService;

public class AttributeServiceImplTestNG extends AttributeEntityMgrImplTestNG {

    @Inject
    private AttributeService attributeService;

    protected List<Attribute> getAttributesByNameAndTableName(String attributeName, String tableName) {
        return attributeService.getAttributesByNameAndTableName(attributeName, tableName);
    }

    protected long countByTablePid(Long tablePid) {
        return attributeService.countByTablePid(tablePid);
    }

    protected List<Attribute> findByTablePid(Long tablePid) {
        return attributeService.findByTablePid(tablePid);
    }

    protected List<Attribute> findByTablePid(Long tablePid, Pageable pageable) {
        return attributeService.findByTablePid(tablePid, pageable);
    }
}
