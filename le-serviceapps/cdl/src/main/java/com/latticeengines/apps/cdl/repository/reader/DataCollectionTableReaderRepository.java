package com.latticeengines.apps.cdl.repository.reader;

import java.util.List;

import javax.persistence.Tuple;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionTable;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

public interface DataCollectionTableReaderRepository extends BaseJpaRepository<DataCollectionTable, Long> {

    @Query("select table.name from DataCollectionTable")
    List<String> findAllTableName();

    @Query("select t from DataCollectionTable t where dataCollection.name = :collectionName "
            + "and role = :tableRole and version = :version and signature = :signature")
    DataCollectionTable findFirstBySignature(@Param("collectionName") String collectionName,
            @Param("tableRole") TableRoleInCollection tableRole, @Param("version") DataCollection.Version version,
            @Param("signature") String signature);

    @Query("select signature, table.name from DataCollectionTable where dataCollection.name = :collectionName "
            + "and role = :tableRole and version = :version and signature in :signatures")
    List<Tuple> findTablesInRole(@Param("collectionName") String collectionName,
            @Param("tableRole") TableRoleInCollection tableRole, @Param("version") DataCollection.Version version,
            @Param("signatures") List<String> signatures);

    DataCollectionTable findByPid(Long dataCollectionTablePid);
}
