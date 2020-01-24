package com.latticeengines.metadata.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.metadata.entitymgr.AttributeEntityMgr;
import com.latticeengines.metadata.service.AttributeService;

@Component("attributeService")
public class AttributeServiceImpl implements AttributeService {

    @Inject
    private AttributeEntityMgr attributeEntityMgr;

    @Override
    public List<Attribute> getAttributesByNamesAndTableName(List<String> attributeNames, String tableName,
                                                            int tableTypeCode) {
        return attributeEntityMgr.getAttributesByNamesAndTableName(attributeNames, tableName, tableTypeCode);
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
