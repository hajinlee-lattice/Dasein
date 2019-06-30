package com.latticeengines.metadata.repository.db;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.Attribute;

@Transactional(readOnly = true)
public interface AttributeRepository extends BaseJpaRepository<Attribute, Long> {

    long countByTable_Pid(Long tablePid);

    List<Attribute> findByTable_Pid(Long tablePid, Pageable pageable);

    List<Attribute> findByTable_Pid(Long tablePid);

    @Query("select att from Attribute att where att.name in ?1 and att.table.name = ?2 and att.table.tableTypeCode = ?3")
    List<Attribute> getByNamesAndTableName(List<String> attributeNames, String tableName, int tableTypeCode);

    List<Attribute> findByName(String name);

}
