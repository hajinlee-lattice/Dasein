package com.latticeengines.apps.cdl.service.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishRequest;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

@Component("checkpointService")
public class CheckpointService extends CheckpointServiceBase {

    private static final Logger log = LoggerFactory.getLogger(CheckpointService.class);

    protected void cloneAnUploadTables(String checkpoint, String checkpointVersion) throws IOException {
        dataFeedProxy.getDataFeed(mainTestTenant.getId());
        String[] tenantNames = new String[1];

        DataCollection.Version activeVersion = getCheckpointVersion(checkpoint);

        Map<String, String> redshiftTablesToClone = new HashMap<>();
        Set<String> uploadedTables = new HashSet<>();
        RedshiftService redshiftService = redshiftPartitionService.getBatchUserService(null);
        for (DataCollection.Version version : DataCollection.Version.values()) {
            for (TableRoleInCollection role : TableRoleInCollection.values()) {
                List<Table> tables = parseCheckpointTable(checkpoint, role.name(), version, tenantNames);
                List<String> tableNames = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(tables)) {
                    for (Table table : tables) {
                        if (table != null) {
                            log.info("Creating table {} for {} in version {}.", table.getName(), role, version);
                            if (!uploadedTables.contains(table.getName())) {
                                metadataProxy.createTable(mainTestTenant.getId(), table.getName(), table);
                                uploadedTables.add(table.getName());
                            }
                            tableNames.add(table.getName());
                            if (activeVersion.equals(version)) {
                                String redshiftTable = checkpointRedshiftTableName(checkpoint, role, checkpointVersion);
                                if (redshiftService.hasTable(redshiftTable)) {
                                    redshiftTablesToClone.put(redshiftTable, table.getName());
                                }
                                DynamoDataUnit dynamoDataUnit = parseDynamoDataUnit(checkpoint, role.name(), version);
                                if (dynamoDataUnit != null) {
                                    if (StringUtils.isBlank(dynamoDataUnit.getLinkedTable())) {
                                        dynamoDataUnit.setLinkedTable(dynamoDataUnit.getName());
                                    }
                                    if (StringUtils.isBlank(dynamoDataUnit.getLinkedTenant())) {
                                        dynamoDataUnit.setLinkedTenant(dynamoDataUnit.getTenant());
                                    }
                                    dynamoDataUnit.setName(table.getName());
                                    dynamoDataUnit
                                            .setTenant(CustomerSpace.shortenCustomerSpace(mainTestTenant.getId()));
                                    log.info("Creating data unit " + JsonUtils.serialize(dynamoDataUnit));
                                    dataUnitProxy.create(mainTestTenant.getId(), dynamoDataUnit);
                                }
                            }
                        }
                    }
                }

                if (CollectionUtils.isNotEmpty(tableNames)) {
                    if (tableNames.size() == 1) {
                        dataCollectionProxy.upsertTable(mainTestTenant.getId(), tableNames.get(0), role, version);
                    } else {
                        dataCollectionProxy.upsertTables(mainTestTenant.getId(), tableNames, role, version);
                    }
                }
            }
            StatisticsContainer statisticsContainer = parseCheckpointStatistics(checkpoint, version);
            if (statisticsContainer != null) {
                dataCollectionProxy.upsertStats(mainTestTenant.getId(), statisticsContainer);
            }
            DataCollectionStatus dataCollectionStatus = parseDataCollectionStatus(checkpoint, version);
            if (dataCollectionStatus != null) {
                dataCollectionProxy.saveOrUpdateDataCollectionStatus(mainTestTenant.getId(), dataCollectionStatus,
                        version);
            }
        }

        String cloneRedshiftMsg = String.format("Clone %d redshift tables", MapUtils.size(redshiftTablesToClone));
        try (PerformanceTimer timer = new PerformanceTimer(cloneRedshiftMsg)) {
            cloneRedshiftTables(redshiftTablesToClone);
        }
        uploadCheckpointHdfs(checkpoint);
        if (copyToS3) {
            uploadCheckpointS3(checkpoint);
        }

        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Active.name());
        resumeDbState();

        copyEntitySeedTables(checkpoint, checkpointVersion);

        dataCollectionProxy.switchVersion(mainTestTenant.getId(), activeVersion);
        log.info("Switch active version to {}.", activeVersion);
    }

    private List<Table> parseCheckpointTable(String checkpoint, String roleName, DataCollection.Version version,
            String[] tenantNames) throws IOException {
        String jsonFilePath = String.format("%s/%s/%s/tables/%s.json", checkpointDir, checkpoint, version.name(),
                roleName);
        log.info("Checking table json file path {}.", jsonFilePath);
        File jsonFile = new File(jsonFilePath);
        if (!jsonFile.exists()) {
            return null;
        }

        log.info("Parse check point {} table {} of version {}.", checkpoint, roleName, version.name());
        List<Table> tables = new ArrayList<>();
        ArrayNode arrNode = (ArrayNode) om.readTree(jsonFile);
        Iterator<JsonNode> iter = arrNode.elements();
        while (iter.hasNext()) {
            JsonNode json = iter.next();
            String hdfsPath = json.get("extracts_directory").asText();
            if (StringUtils.isBlank(hdfsPath)) {
                hdfsPath = json.get("extracts").get(0).get("path").asText();
                if (hdfsPath.endsWith(".avro") || hdfsPath.endsWith("/")) {
                    hdfsPath = hdfsPath.substring(0, hdfsPath.lastIndexOf("/"));
                }
            }
            log.info("Parse extract path {}.", hdfsPath);
            Pattern pattern = Pattern.compile(PATH_PATTERN);
            Matcher matcher = pattern.matcher(hdfsPath);
            String str = JsonUtils.serialize(json);
            str = str.replaceAll(POD_DEFAULT, String.format(POD_PATTERN, podId));
            str = str.replaceAll(POD_QA, String.format(POD_PATTERN, podId));
            if (matcher.find()) {
                tenantNames[0] = matcher.group(1);
                log.info("Found tenant name {} in json.", tenantNames[0]);
            } else {
                log.info("Cannot find tenant for {}.", tenantNames[0]);
            }

            if (tenantNames[0] != null) {
                String testTenant = CustomerSpace.parse(mainTestTenant.getId()).getTenantId();
                String hdfsPathSegment1 = hdfsPath.substring(0, hdfsPath.lastIndexOf("/"));
                String hdfsPathSegment2 = hdfsPath.substring(hdfsPath.lastIndexOf("/"));
                if (hdfsPathSegment2.contains(tenantNames[0])) {
                    String hdfsPathIntermediatePattern = hdfsPathSegment1.replaceAll(tenantNames[0], testTenant) //
                            + "/$$TABLE_DATA_DIR$$";
                    String hdfsPathFinal = hdfsPathSegment1.replaceAll(tenantNames[0], testTenant) + hdfsPathSegment2;
                    str = str.replaceAll(hdfsPath, hdfsPathIntermediatePattern);
                    str = str.replaceAll(tenantNames[0], testTenant);
                    str = str.replaceAll(hdfsPathIntermediatePattern, hdfsPathFinal);
                } else {
                    str = str.replaceAll(tenantNames[0], testTenant);
                }
            }
            tables.add(JsonUtils.deserialize(str, Table.class));
        }

        return tables;
    }

    public void saveCheckpoint(String checkpointName, String checkpointVersion) throws IOException {
        String rootDir = String.format("%s/%s", LOCAL_CHECKPOINT_DIR, checkpointName);
        FileUtils.deleteQuietly(new File(rootDir));
        FileUtils.forceMkdirParent(new File(rootDir));

        downloadHdfsData(checkpointName);

        DataCollection.Version active = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
        for (DataCollection.Version version : DataCollection.Version.values()) {
            String tablesDir = String.format(TABLE_DIR, LOCAL_CHECKPOINT_DIR, checkpointName, version.name());
            FileUtils.forceMkdir(new File(tablesDir));
            for (TableRoleInCollection role : TableRoleInCollection.values()) {
                saveTableIfExists(role, version, checkpointName);
                if (active.equals(version)) {
                    saveRedshiftTableIfExists(role, version);
                    saveDynamoTableIfExists(checkpointName, role, version);
                }
            }
            saveStatsIfExists(version, checkpointName);
            saveDataCollectionStatus(version, checkpointName);
        }

        printSaveRedshiftStatements(checkpointName, checkpointVersion);
        saveCheckpointVersion(checkpointName);
        printPublishEntityRequest(checkpointName, checkpointVersion);
    }

    public void saveCheckpoint(String checkpointName, String checkpointVersion, String customerSpace)
            throws IOException {
        String rootDir = String.format("%s/%s", LOCAL_CHECKPOINT_DIR, checkpointName);
        FileUtils.deleteQuietly(new File(rootDir));
        FileUtils.forceMkdirParent(new File(rootDir));

        downloadHdfsData(checkpointName);

        DataCollection.Version active = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
        for (DataCollection.Version version : DataCollection.Version.values()) {
            String tablesDir = String.format(TABLE_DIR, LOCAL_CHECKPOINT_DIR, checkpointName, version.name());
            FileUtils.forceMkdir(new File(tablesDir));
            for (TableRoleInCollection role : TableRoleInCollection.values()) {
                saveTableIfExists(role, version, checkpointName);
                if (active.equals(version)) {
                    saveRedshiftTableIfExists(role, version);
                    saveDynamoTableIfExists(checkpointName, role, version);
                }
            }
            saveStatsIfExists(version, checkpointName);
            saveDataCollectionStatus(version, checkpointName);
        }

        printSaveRedshiftStatements(checkpointName, checkpointVersion);
        saveCheckpointVersion(checkpointName);
        // Save Workflow Execution Context.
        saveWorkflowExecutionContext(checkpointName, customerSpace);
        printPublishEntityRequest(checkpointName, checkpointVersion);
    }

    private void saveDataCollectionStatus(DataCollection.Version version, String checkpoint) throws IOException {
        DataCollectionStatus dataCollectionStatus = dataCollectionProxy
                .getOrCreateDataCollectionStatus(mainTestTenant.getId(), version);
        String jsonFile = String.format(DATA_COLLECTION_STATUS_JSONFILE_FORMAT, LOCAL_CHECKPOINT_DIR, checkpoint,
                version.name());
        om.writeValue(new File(jsonFile), dataCollectionStatus);
        log.info("Save DataCollection Status at version {} to {}.", version, jsonFile);
    }

    private void printSaveRedshiftStatements(String checkpointName, String checkpointVersion) {
        if (MapUtils.isNotEmpty(savedRedshiftTables)) {
            StringBuilder msg = new StringBuilder(
                    "If you are going to save the checkpoint to version " + checkpointVersion);
            msg.append(", you can run following statements in redshift:\n\n");
            List<String> dropTables = new ArrayList<>();
            List<String> renameTables = new ArrayList<>();
            savedRedshiftTables.forEach((role, table) -> {
                String tgtTable = checkpointRedshiftTableName(checkpointName, role, checkpointVersion);
                dropTables.add("drop table if exists " + tgtTable + ";");
                renameTables.add(String.format("alter table %s rename to %s;", table, tgtTable));
            });
            for (String statement : dropTables) {
                msg.append(statement).append("\n");
            }
            for (String statement : renameTables) {
                msg.append(statement).append("\n");
            }
            log.info(msg.toString());
        }
    }

    /**
     * For entity match enabled PA, when saving checkpoint for match lookup/seed table, need to publish all the
     * preceding checkpoints' staging lookup/seed table instead of only current tenant.
     *
     * Reason is in current tenant's staging table, only entries which are touched in match job exist, which means the
     * staging table doesn't have complete entity universe for the tenant. Although serving table has complete entity
     * universe, serving table doesn't support scan due to lookup performance concern.
     *
     * @param checkpointName
     * @param checkpointVersion
     */
    @VisibleForTesting
    void printPublishEntityRequest(String checkpointName, String checkpointVersion) {
        if (!isEntityMatchEnabled()) {
            return;
        }
        try {
            StringBuilder msg = new StringBuilder("\nTo save Entity Match Seed Table to checkpoint of version "
                    + checkpointVersion + ", you need to run the following HTTP Requests:\n");
            for (BusinessEntity businessEntity : Arrays.asList(BusinessEntity.Account, BusinessEntity.Contact)) {
                msg.append("POST " + matchapiHostPort + "/match/matches/entity/versions\n");
                msg.append("Body:\n");
                BumpVersionRequest request = new BumpVersionRequest();
                String destTenantId = getCheckPointTenantId(checkpointName, checkpointVersion, businessEntity.name());
                Tenant destTenant = new Tenant(CustomerSpace.parse(destTenantId).toString());
                request.setTenant(destTenant);
                request.setEnvironments(Arrays.asList(EntityMatchEnvironment.STAGING, EntityMatchEnvironment.SERVING));
                msg.append(om.writerWithDefaultPrettyPrinter().writeValueAsString(request) + "\n");
            }

            msg.append(
                    "Following APIs might take 5+ mins to respond -- Could track publish progress in tomcat console\n");
            for (BusinessEntity businessEntity : Arrays.asList(BusinessEntity.Account, BusinessEntity.Contact)) {
                msg.append("POST " + matchapiHostPort + "/match/matches/entity/publish/list\n");
                msg.append("Body:\n");

                List<EntityPublishRequest> requests = new ArrayList<>();
                String destTenantId = getCheckPointTenantId(checkpointName, checkpointVersion, businessEntity.name());
                Tenant destTenant = new Tenant(CustomerSpace.parse(destTenantId).toString());
                List<Tenant> srcTenants = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(precedingCheckpoints)) {
                    srcTenants.addAll(precedingCheckpoints.stream()
                            .map(cp -> new Tenant(CustomerSpace
                                    .parse(getCheckPointTenantId(cp, checkpointVersion, businessEntity.name()))
                                    .toString()))
                            .collect(Collectors.toList()));
                }
                srcTenants.add(mainTestTenant);

                for (Tenant srcTenant : srcTenants) {
                    EntityPublishRequest request = new EntityPublishRequest();
                    request.setEntity(businessEntity.name());
                    request.setSrcTenant(srcTenant);

                    request.setDestTenant(destTenant);
                    request.setDestEnv(EntityMatchEnvironment.STAGING);
                    request.setDestTTLEnabled(false);
                    request.setBumpupVersion(false);
                    requests.add(request);

                    request = om.readValue(om.writeValueAsString(request), EntityPublishRequest.class);
                    request.setDestEnv(EntityMatchEnvironment.SERVING);
                    requests.add(request);
                }
                msg.append(om.writerWithDefaultPrettyPrinter().writeValueAsString(requests) + "\n");
            }
            log.info(msg.toString());
        } catch (IOException e) {
            log.error("Failed to print EntityPublishRequest:\n" + e.getMessage(), e);
        }
    }
}
