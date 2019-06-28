package com.latticeengines.metadata.entitymgr;

import java.util.List;

import org.springframework.data.domain.Pageable;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.metadata.Attribute;

public interface AttributeEntityMgr extends BaseEntityMgrRepository<Attribute, Long> {

    List<Attribute> getAttributesByNameAndTableName(String attributeName, String tableName);

    long countByTablePid(Long tablePid);

    List<Attribute> findByTablePid(Long tablePid);

    List<Attribute> findByTablePid(Long tablePid, Pageable pageable);

}
