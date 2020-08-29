package com.latticeengines.datacloud.match.repository.reader;

import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.datacloud.manage.ContactMasterTpsColumn;

@Transactional(readOnly = true, propagation = Propagation.REQUIRES_NEW)
public interface ContactMasterTpsColumnRepository extends BaseJpaRepository<ContactMasterTpsColumn, Long> {

    @Query(value = "SELECT a.ColumnName from ContactMasterTpsColumn a where a.MatchDestination = ?1", nativeQuery = true)
    String findColumnNameByMatchDestination(String matchDest);

    @Query(value = "SELECT a.JavaClass from ContactMasterTpsColumn a where a.columnName = ?1", nativeQuery = true)
    String findJavaClassByColumnName(String columnName);
}
