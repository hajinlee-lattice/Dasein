package com.latticeengines.apps.cdl.repository.reader;

import java.util.List;

import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.DataCollectionTable;

public interface DataCollectionTableReaderRepository extends BaseJpaRepository<DataCollectionTable, Long> {

    @Query("select table.name from DataCollectionTable")
    List<String> findAllTableName();

    DataCollectionTable findByPid(Long dataCollectionTablePid);
}
