package com.latticeengines.metadata.entitymgr;

import java.util.List;

import org.springframework.data.domain.Pageable;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.metadata.Attribute;

public interface AttributeEntityMgr extends BaseEntityMgrRepository<Attribute, Long> {

    List<Attribute> getAttributesByNamesAndTableName(List<String> attributeNames, String tableName, int tableTypeCode);

    long countByTablePid(Long tablePid);

    List<Attribute> findByTablePid(Long tablePid);

    List<Attribute> findByTablePid(Long tablePid, Pageable pageable);

}
