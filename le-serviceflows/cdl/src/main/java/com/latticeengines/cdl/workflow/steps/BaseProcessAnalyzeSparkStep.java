package com.latticeengines.cdl.workflow.steps;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.MigrationTrack;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicy;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicyTimeUnit;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.RedshiftExportConfig;
import com.latticeengines.domain.exposed.util.RetentionPolicyUtil;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.serviceflows.workflow.dataflow.BaseSparkStep;
import com.latticeengines.serviceflows.workflow.util.SparkUtils;
import com.latticeengines.spark.exposed.service.LivySessionService;
import com.latticeengines.spark.exposed.service.SparkJobService;

// base PA step: spark enabled, has the concept of active, inactive version
// and various frequently used PA methods
public abstract class BaseProcessAnalyzeSparkStep<T extends BaseProcessEntityStepConfiguration> extends BaseSparkStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseProcessAnalyzeSparkStep.class);

    // The date format pattern desired by the UI for Last Data Refresh Attribute field.
    private static final DateTimeFormatter REFRESH_DATE_FORMATTER = DateTimeFormatter.ofPattern("MMMM d, yyyy");

    @Inject
    protected CDLProxy cdlProxy;

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    private PeriodProxy periodProxy;

    @Inject
    private CloneTableService cloneTableService;

    @Inject
    private LivySessionService sessionService;

    @Inject
    private SparkJobService sparkJobService;

    // The date that the Process/Analyze pipeline was run as a string.
    protected String evaluationDateStr = null;
    // The timestamp representing the beginning of the day that the Process/Analyze pipeline was run.  Used for date
    // attribute profiling.
    protected Long evaluationDateAsTimestamp = null;


    protected String customerSpaceStr;
    protected DataCollection.Version active;
    protected DataCollection.Version inactive;

    protected void bootstrap() {
        customerSpace = configuration.getCustomerSpace();
        customerSpaceStr = customerSpace.toString();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        configureCloneService();
    }

    // try to get a table role, first in inactive then in active
    protected Table attemptGetTableRole(TableRoleInCollection tableRole, boolean failOnMissing) {
        Table table = dataCollectionProxy.getTable(customerSpace.toString(), tableRole, inactive);
        if (table == null) {
            table = dataCollectionProxy.getTable(customerSpace.toString(), tableRole, active);
            if (table != null) {
                log.info("Found {} table in active version {}", tableRole, active);
            }
        } else {
            log.info("Found {} table in inactive version {}", tableRole, inactive);
        }
        if (table == null && failOnMissing) {
            throw new IllegalStateException("Neither active nor inactive table for " + tableRole + " exists");
        }
        return table;
    }

    private void configureCloneService() {
        cloneTableService.setActiveVersion(active);
        cloneTableService.setCustomerSpace(customerSpace);
        DataCollectionStatus dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (dcStatus != null && dcStatus.getDetail() != null) {
            cloneTableService.setRedshiftPartition(dcStatus.getRedshiftPartition());
        }
    }

    protected void setEvaluationDateStrAndTimestamp() {
        // Convert the evaluation date (generally the current date which is when the pipeline is running) to a
        // timestamp.
        evaluationDateStr = findEvaluationDate();
        LocalDate evaluationDate;
        if (!StringUtils.isBlank(evaluationDateStr)) {
            try {
                evaluationDate = LocalDate.parse(evaluationDateStr, DateTimeFormatter.ISO_DATE);
            } catch (DateTimeParseException e) {
                log.error("Could not parse evaluation date string \"" + evaluationDateStr
                        + "\" from Period Proxy as an ISO formatted date", e);
                evaluationDate = LocalDate.now();
                evaluationDateStr = evaluationDate.format(REFRESH_DATE_FORMATTER);
            }
        } else {
            log.warn("Evaluation Date from Period Proxy is blank.  Profile Account will generate date");
            evaluationDate = LocalDate.now();
            evaluationDateStr = evaluationDate.format(REFRESH_DATE_FORMATTER);
        }
        evaluationDateAsTimestamp = evaluationDate.atStartOfDay(ZoneId.of("UTC")).toInstant().toEpochMilli();
        log.info("Evaluation date for Profile Account date attributes: " + evaluationDateStr);
        log.info("Evaluation timestamp for Profile Account date attributes: " + evaluationDateAsTimestamp);
    }

    protected String findEvaluationDate() {
        String evaluationDate = getStringValueFromContext(CDL_EVALUATION_DATE);
        if (StringUtils.isBlank(evaluationDate)) {
            log.error("Failed to find evaluation date from workflow context");
            evaluationDate = periodProxy.getEvaluationDate(customerSpace.toString());
            if (StringUtils.isBlank(evaluationDate)) {
                log.error("Failed to get evaluation date from Period Proxy.");
            }
        }
        return evaluationDate;
    }

    protected boolean isChanged(TableRoleInCollection tableRole) {
        String inactiveName = dataCollectionProxy.getTableName(customerSpace.toString(), tableRole, inactive);
        log.info("inactiveName=" + inactiveName + " tableRole=" + tableRole.name());
        if (StringUtils.isNotBlank(inactiveName)) {
            String activeName = dataCollectionProxy.getTableName(customerSpace.toString(), tableRole, active);
            log.info("activeName=" + activeName + " tableRole=" + tableRole.name());
            return !inactiveName.equals(activeName);
        } else {
            // consider no change if no inactive version, no matter whether active version exists
            return false;
        }
    }

    protected boolean isChanged(TableRoleInCollection tableRole, String changeListCtxKey) {
        boolean changed;
        String inactiveName = dataCollectionProxy.getTableName(customerSpace.toString(), tableRole, inactive);
        if (StringUtils.isNotBlank(inactiveName)) {
            String activeName = dataCollectionProxy.getTableName(customerSpace.toString(), tableRole, active);
            changed = !inactiveName.equals(activeName);
            log.info("tableRole=" + tableRole + " activeName=" + activeName + " inactiveName=" + inactiveName
                    + " entitty="
                    + configuration.getMainEntity());
        } else {
            // consider no change if no inactive version, no matter whether active version exists
            changed = false;
        }
        log.info("tableRole=" + tableRole.name() + " changeListCtxKey=" + changeListCtxKey + " changed=" + changed);
        if (changed && StringUtils.isNotBlank(changeListCtxKey)) {
            String tableName = getStringValueFromContext(changeListCtxKey);
            if (StringUtils.isNotBlank(tableName)) {
                Table changeListTbl = metadataProxy.getTableSummary(customerSpaceStr, tableName);
                if (changeListTbl != null) {
                    long cnt = changeListTbl.toHdfsDataUnit("ChangeList").getCount();
                    if (cnt <= 0) {
                        log.info("There are 0 entries in change list {}, so no real changes.", changeListCtxKey);
                        changed = false;
                    } else {
                        log.info("There are {} entries in the change list {}, so it is really changed.", //
                                cnt, changeListCtxKey);
                    }
                }
            }
        }
        return changed;
    }

    protected void linkInactiveTable(TableRoleInCollection tableRole) {
        cloneTableService.linkInactiveTable(tableRole);
    }

    protected void resetEntity(BusinessEntity servingEntity) {
        Set<BusinessEntity> resetEntities = getSetObjectFromContext(RESET_ENTITIES, BusinessEntity.class);
        if (CollectionUtils.isEmpty(resetEntities)) {
            resetEntities = new HashSet<>();
        } else {
            resetEntities = new HashSet<>(resetEntities);
        }
        resetEntities.add(servingEntity);
        putObjectInContext(RESET_ENTITIES, resetEntities);
    }

    // reset means remove this entity from serving stores
    protected boolean isToReset(BusinessEntity servingEntity) {
        Set<BusinessEntity> resetEntities = getSetObjectFromContext(RESET_ENTITIES, BusinessEntity.class);
        return CollectionUtils.isNotEmpty(resetEntities) && resetEntities.contains(servingEntity);
    }

    protected Attribute copyMasterAttr(Map<String, Attribute> masterAttrs, Attribute attr0) {
        Attribute attr = masterAttrs.get(attr0.getName());
        if (attr0.getNumOfBits() != null && attr0.getNumOfBits() > 0) {
            attr.setNullable(Boolean.TRUE);
            attr.setPhysicalName(attr0.getPhysicalName());
            attr.setNumOfBits(attr0.getNumOfBits());
            attr.setBitOffset(attr0.getBitOffset());
            attr.setPhysicalDataType(Schema.Type.STRING.getName());
        }
        if (CollectionUtils.isEmpty(attr.getGroupsAsList())) {
            attr.setGroupsViaList(Collections.singletonList(ColumnSelection.Predefined.Segment));
        } else if (!attr.getGroupsAsList().contains(ColumnSelection.Predefined.Segment)) {
            attr.getGroupsAsList().add(ColumnSelection.Predefined.Segment);
        }
        return attr;
    }

    protected void exportTableRoleToRedshift(Table table, TableRoleInCollection tableRole) {
        String distKey = tableRole.getDistKey();
        List<String> sortKeys = new ArrayList<>(tableRole.getSortKeys());

        String partition = null;
        DataCollectionStatus dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (dcStatus != null && dcStatus.getDetail() != null) {
            partition = dcStatus.getRedshiftPartition();
        }

        String tableName = table.getName();
        RedshiftExportConfig config = new RedshiftExportConfig();
        config.setTableName(tableName);
        config.setDistKey(distKey);
        config.setSortKeys(sortKeys);
        config.setInputPath(PathUtils.toAvroGlob(table.getExtracts().get(0).getPath()));
        config.setClusterPartition(partition);
        config.setUpdateMode(false);

        Table summary = metadataProxy.getTableSummary(customerSpace.toString(), tableName);
        if (CollectionUtils.isNotEmpty(summary.getExtracts())) {
            Extract extract = summary.getExtracts().get(0);
            Long count = extract.getProcessedRecords();
            if (count != null && count > 0) {
                config.setExpectedCount(count);
            }
        }

        addToListInContext(TABLES_GOING_TO_REDSHIFT, config, RedshiftExportConfig.class);
    }

    protected <V> void updateEntityValueMapInContext(String key, V value, Class<V> clz) {
        updateEntityValueMapInContext(configuration.getMainEntity(), key, value, clz);
    }

    protected <V> void updateEntityValueMapInContext(BusinessEntity entity, String key, V value, Class<V> clz) {
        Map<BusinessEntity, V> entityValueMap = getMapObjectFromContext(key, BusinessEntity.class, clz);
        if (entityValueMap == null) {
            entityValueMap = new HashMap<>();
        }
        entityValueMap.put(entity, value);
        putObjectInContext(key, entityValueMap);
    }

    protected void updateEntitySetInContext(String key, BusinessEntity entity) {
        Set<BusinessEntity> entitySet = getSetObjectFromContext(key, BusinessEntity.class);
        if (entitySet == null) {
            entitySet = new HashSet<>();
        }
        entitySet.add(entity);
        putObjectInContext(key, entitySet);
    }

    protected boolean inMigrationMode() {
        MigrationTrack.Status status = metadataProxy.getMigrationStatus(customerSpace.toString());
        log.info("Tenant's migration status is {}.", status);
        boolean migrationMode = MigrationTrack.Status.STARTED.equals(status);
        log.info("Migration mode is {}", migrationMode ? "on" : "off");
        return migrationMode;
    }

    /*-
     * get all possible values for EntityCreatedSource attribute
     */
    protected Set<String> getCreatedSourceNames() {
        List<S3ImportSystem> systems = cdlProxy.getS3ImportSystemList(customerSpace.toString());
        return CollectionUtils.emptyIfNull(systems) //
                .stream() //
                .filter(Objects::nonNull) //
                .filter(sys -> StringUtils.isNotBlank(sys.getDisplayName())) //
                .map(S3ImportSystem::getDisplayName) //
                .collect(Collectors.toSet());
    }

    protected void addShortRetentionToTable(String tableName) {
        RetentionPolicy retentionPolicy = RetentionPolicyUtil.toRetentionPolicy(3, RetentionPolicyTimeUnit.DAY);
        metadataProxy.updateDataTablePolicy(customerSpace.toString(), tableName, retentionPolicy);
    }

    protected boolean tableExist(Map<String, Table> tables) {
        return tableExist(new ArrayList<>(tables.values()));
    }

    protected boolean tableExist(List<Table> tables) {
        return CollectionUtils.isNotEmpty(tables) && tables.stream().allMatch(this::tableExist);
    }

    protected boolean tableExist(Table table) {
        return table != null;
    }

    protected boolean tableInHdfs(Map<String, Table> tables, boolean partitioned) {
        return tableInHdfs(new ArrayList<>(tables.values()), partitioned);
    }

    protected boolean tableInHdfs(List<Table> tables, boolean partitioned) {
        return CollectionUtils.isNotEmpty(tables) && tables.stream().allMatch(table -> tableInHdfs(table, partitioned));
    }

    protected boolean tableInHdfs(Table table, boolean partitioned) {
        String tablePath = partitioned ? table.getExtracts().get(0).getPath() : table.getExtractsDirectory();
        boolean result = false;
        try {
            result = HdfsUtils.fileExists(yarnConfiguration, tablePath);
        } catch (IOException e) {
            log.warn("Failed to check if table exists with path {}", tablePath);
            e.printStackTrace();
        }
        if (!result) {
            log.warn("Found table {} in database but not in hdfs.", table.getName());
        }
        return result;
    }

    protected boolean tableInRoleExists(TableRoleInCollection tableRole, DataCollection.Version version) {
        String tableName = dataCollectionProxy.getTableName(customerSpace.toString(), tableRole, version);
        if (StringUtils.isBlank(tableName)) {
            return false;
        }
        Table table = metadataProxy.getTable(customerSpace.toString(), tableName);
        return table != null;
    }

    protected long countRawEntitiesInHdfs(TableRoleInCollection tableRole, DataCollection.Version version) {
        String tableName = dataCollectionProxy.getTableName(customerSpace.toString(), tableRole, version);
        if (StringUtils.isBlank(tableName)) {
            return 0L;
        }
        Table table = metadataProxy.getTable(customerSpace.toString(), tableName);
        if (table == null) {
            log.error("Cannot find table " + tableName);
            return 0L;
        }
        Long count = table.getExtracts().get(0).getProcessedRecords();
        if (count == null || count <= 0) {
            String hdfsPath = table.getExtracts().get(0).getPath();
            hdfsPath = PathUtils.toAvroGlob(hdfsPath);
            log.info("Count records in HDFS " + hdfsPath);
            count = SparkUtils.countRecordsInGlobs(sessionService, sparkJobService, yarnConfiguration, hdfsPath);
        }
        log.info(String.format("Table role %s version %s has %d entities.", tableRole.name(), version.name(), count));
        return count;
    }
}
