package com.latticeengines.metadata.service;

import java.util.List;

import org.springframework.data.domain.Pageable;

import com.latticeengines.domain.exposed.metadata.Attribute;

public interface AttributeService {

    List<Attribute> getAttributesByNamesAndTableName(List<String> attributeNames, String tableName, int tableTypeCode);

    long countByTablePid(Long tablePid);

    List<Attribute> findByTablePid(Long tablePid);

    List<Attribute> findByTablePid(Long tablePid, Pageable pageable);
}
