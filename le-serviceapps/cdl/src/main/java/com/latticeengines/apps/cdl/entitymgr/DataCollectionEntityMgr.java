package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.PersistenceException;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionTable;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

public interface DataCollectionEntityMgr extends BaseEntityMgr<DataCollection> {
    DataCollection createDefaultCollection();

    DataCollection findDefaultCollection();

    DataCollection.Version findActiveVersion();

    DataCollection.Version findInactiveVersion();

    DataCollection getDataCollection(String name);

    List<Table> findTablesOfRole(String collectionName, TableRoleInCollection tableRole, DataCollection.Version version);

    List<String> findTableNamesOfRole(String collectionName, TableRoleInCollection tableRole, DataCollection.Version version);

    /*-
     * Retrieve a map of signature -> table name in target collection
     */
    Map<String, String> findTableNamesOfOfRoleAndSignatures(@NotNull String collectionName,
            @NotNull TableRoleInCollection tableRole, @NotNull DataCollection.Version version, Set<String> signatures);

    /*-
     * Retrieve a map of signature -> table in target collection.
     */
    Map<String, Table> findTablesOfRoleAndSignatures(@NotNull String collectionName,
            @NotNull TableRoleInCollection tableRole, @NotNull DataCollection.Version version, Set<String> signatures);

    List<String> getAllTableName();

    DataCollectionTable upsertTableToCollection(String collectionName, String tableName, TableRoleInCollection role,
                                                DataCollection.Version version);

    /**
     * Create a {@link DataCollectionTable}. If signature is not {@code null}, [
     * tenant, role, signature, version ] needs to be unique.
     *
     * @param collectionName
     *            target collection, must exist
     * @param tableName
     *            name of table to added to collection
     * @param role
     *            target role
     * @param signature
     *            target signature, can be {@code null}
     * @param version
     *            target version
     * @throws PersistenceException
     *             if unique constraint is violated
     * @return created {@link DataCollectionTable}, {@code null} if given tableName
     *         does not exist
     */
    DataCollectionTable addTableToCollection(@NotNull String collectionName, @NotNull String tableName,
            @NotNull TableRoleInCollection role, String signature, @NotNull DataCollection.Version version);

    /*
     * link to target table with specific role/signature/version, replace existing
     * link if it already exists. input table and collection must exist.
     */
    DataCollectionTable upsertTableToCollection(@NotNull String collectionName, @NotNull String tableName,
            @NotNull TableRoleInCollection role, @NotNull String signature, @NotNull DataCollection.Version version);

    void removeTableFromCollection(String collectionName, String tableName, DataCollection.Version version);

    void removeAllTablesFromCollection(String customerSpace, DataCollection.Version version);

    void upsertStatsForMasterSegment(String collectionName, StatisticsContainer statisticsContainer);

    List<DataCollectionTable> findTablesFromCollection(String collectionName, String tableName);

    Map<TableRoleInCollection, Map<DataCollection.Version, List<String>>> findTableNamesOfAllRole(String collectionName, TableRoleInCollection tableRole, DataCollection.Version version);

    DataCollectionTable findDataCollectionTableByPid(Long dataCollectionId);

}
