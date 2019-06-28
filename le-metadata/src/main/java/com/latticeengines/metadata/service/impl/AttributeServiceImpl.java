package com.latticeengines.metadata.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.metadata.entitymgr.AttributeEntityMgr;
import com.latticeengines.metadata.service.AttributeService;

@Component("attributeService")
public class AttributeServiceImpl implements AttributeService {

    @Autowired
    private AttributeEntityMgr attributeEntityMgr;

    @Override
    public List<Attribute> getAttributesByNameAndTableName(String attributeName, String tableName) {
        return attributeEntityMgr.getAttributesByNameAndTableName(attributeName, tableName);
    }

    @Override
    public long countByTablePid(Long tablePid) {
        return attributeEntityMgr.countByTablePid(tablePid);
    }

    @Override
    public List<Attribute> findByTablePid(Long tablePid) {
        return attributeEntityMgr.findByTablePid(tablePid);
    }

    @Override
    public List<Attribute> findByTablePid(Long tablePid, Pageable pageable) {
        return attributeEntityMgr.findByTablePid(tablePid, pageable);
    }
}
