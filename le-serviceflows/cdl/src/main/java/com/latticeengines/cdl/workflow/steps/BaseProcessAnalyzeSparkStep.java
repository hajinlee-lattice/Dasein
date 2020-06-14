package com.latticeengines.cdl.workflow.steps;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.serviceflows.workflow.dataflow.BaseSparkStep;

// base PA step: spark enabled, has the concept of active, inactive version
// and various frequently used PA methods
public abstract class BaseProcessAnalyzeSparkStep<T extends BaseProcessEntityStepConfiguration> extends BaseSparkStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseProcessAnalyzeSparkStep.class);

    // The date format pattern desired by the UI for Last Data Refresh Attribute field.
    private static final DateTimeFormatter REFRESH_DATE_FORMATTER = DateTimeFormatter.ofPattern("MMMM d, yyyy");

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    private PeriodProxy periodProxy;

    @Inject
    private CloneTableService cloneTableService;

    // The date that the Process/Analyze pipeline was run as a string.
    protected String evaluationDateStr = null;
    // The timestamp representing the beginning of the day that the Process/Analyze pipeline was run.  Used for date
    // attribute profiling.
    protected Long evaluationDateAsTimestamp = null;

    protected DataCollection.Version active;
    protected DataCollection.Version inactive;

    protected void bootstrap() {
        customerSpace = configuration.getCustomerSpace();
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
        if (StringUtils.isNotBlank(inactiveName)) {
            String activeName = dataCollectionProxy.getTableName(customerSpace.toString(), tableRole, active);
            return !inactiveName.equals(activeName);
        } else {
            // consider no change if no inactive version, no matter whether active version exists
            return false;
        }
    }

    protected void linkInactiveTable(TableRoleInCollection tableRole) {
        cloneTableService.linkInactiveTable(tableRole);
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

}
