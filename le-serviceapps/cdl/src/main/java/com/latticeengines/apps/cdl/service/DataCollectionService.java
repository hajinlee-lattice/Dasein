package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.CDLDataSpace;
import com.latticeengines.domain.exposed.cdl.ImportTemplateDiagnostic;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatusHistory;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;

public interface DataCollectionService {

    DataCollection getDataCollection(String customerSpace, String collectionName);

    DataCollection.Version getActiveVersion(String customerSpace);

    DataCollection.Version switchDataCollectionVersion(String customerSpace, String collectionName,
                                                       DataCollection.Version version);

    DataCollection getDefaultCollection(String customerSpace);

    void addStats(String customerSpace, String collectionName, StatisticsContainer container);

    void removeStats(String customerSpace, String collectionName, DataCollection.Version version);

    void upsertTable(String customerSpace, String collectionName, String tableName, TableRoleInCollection tableRole,
                     DataCollection.Version version);

    void upsertTables(String customerSpace, String collectionName, String[] tableNames, TableRoleInCollection tableRole,
            DataCollection.Version version);

    void removeTable(String customerSpace, String collectionName, String tableName, TableRoleInCollection tableRole,
                     DataCollection.Version version);

    List<Table> getTables(String customerSpace, String collectionName, TableRoleInCollection tableRole,
                          DataCollection.Version version);

    List<String> getTableNames(String customerSpace, String collectionName, TableRoleInCollection tableRole,
                               DataCollection.Version version);

    List<String> getAllTableNames();

    StatisticsContainer getStats(String customerSpace, String collectionName, DataCollection.Version version);

    AttributeRepository getAttrRepo(String customerSpace, String collectionName, DataCollection.Version version);

    void resetTable(String customerSpace, String collectionName, TableRoleInCollection tableRole);

    String updateDataCloudBuildNumber(String customerSpace, String collectionName, String dataCloudBuildNumber);

    void clearCache(String customerSpace);

    DataCollectionStatus getOrCreateDataCollectionStatus(String customerSpace, DataCollection.Version version);

    void saveOrUpdateStatus(String customerSpace, DataCollectionStatus detail, DataCollection.Version version);

    DataCollection createDefaultCollection();

    CDLDataSpace createCDLDataSpace(String cutstomerSpace);

    Map<TableRoleInCollection, Map<DataCollection.Version, List<Table>>> getTableRoleMap(String customerSpace, String collectionName);

    List<DataCollectionArtifact> getArtifacts(String customerSpace, DataCollectionArtifact.Status status,
                                              DataCollection.Version version);

    DataCollectionArtifact getLatestArtifact(String customerSpace, String name, DataCollection.Version version);

    DataCollectionArtifact getOldestArtifact(String customerSpace, String name, DataCollection.Version version);

    DataCollectionArtifact createArtifact(String customerSpace, String artifactName, String artifactUrl,
                                          DataCollectionArtifact.Status status, DataCollection.Version version);

    DataCollectionArtifact updateArtifact(String customerSpace, DataCollectionArtifact artifact);

    DataCollectionArtifact deleteArtifact(String customerSpace, String name, DataCollection.Version version,
                                          boolean deleteLatest);

    byte[] downloadDataCollectionArtifact(String customerSpace, String exportId);

    void saveStatusHistory(String customerSpace, DataCollectionStatus status);

    List<DataCollectionStatusHistory> getCollectionStatusHistory(String customerSpace);

    ImportTemplateDiagnostic diagnostic(String customerSpaceStr, Long dataCollectionTablePid);
}
