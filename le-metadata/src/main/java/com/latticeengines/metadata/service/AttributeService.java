package com.latticeengines.metadata.service;

import java.util.List;

import org.springframework.data.domain.Pageable;

import com.latticeengines.domain.exposed.metadata.Attribute;

public interface AttributeService {

    List<Attribute> getAttributesByNameAndTableName(String attributeName, String tableName);

    long countByTablePid(Long tablePid);

    List<Attribute> findByTablePid(Long tablePid);

    List<Attribute> findByTablePid(Long tablePid, Pageable pageable);
}
