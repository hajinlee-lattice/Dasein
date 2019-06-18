package com.latticeengines.cdl.workflow.steps.rebuild;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;

public abstract class BaseSingleEntityProfileStep<T extends BaseProcessEntityStepConfiguration>
        extends ProfileStepBase<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseSingleEntityProfileStep.class);

    protected DataCollection.Version active;
    protected DataCollection.Version inactive;
    protected Boolean tableFromActiveVersion = false;

    protected String profileTablePrefix;
    protected String statsTablePrefix;
    protected String servingStoreTablePrefix;
    protected String servingStoreSortKey;

    protected BusinessEntity entity;
    protected Table masterTable;
    protected boolean publishToRedshift = true;

    protected String profileTableName;
    protected String servingStoreTableName;
    protected String statsTableName;

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        return generateWorkflowConf();
    }

    @Override
    protected void onPostTransformationCompleted() {
        profileTableName = TableUtils.getFullTableName(profileTablePrefix, pipelineVersion);
        servingStoreTableName = TableUtils.getFullTableName(servingStoreTablePrefix, pipelineVersion);

        if (profileTableRole() != null) {
            upsertProfileTable(profileTableName, profileTableRole());
        }

        Table servingStoreTable = metadataProxy.getTable(customerSpace.toString(), servingStoreTableName);
        enrichTableSchema(servingStoreTable);
        metadataProxy.updateTable(customerSpace.toString(), servingStoreTableName, servingStoreTable);
        servingStoreTableName = renameServingStoreTable(servingStoreTable);

        if (publishToRedshift) {
            exportTableRoleToRedshift(servingStoreTableName, getEntity().getServingStore());
        }
        dataCollectionProxy.upsertTable(customerSpace.toString(), servingStoreTableName,
                getEntity().getServingStore(), inactive);

        if (StringUtils.isNotBlank(statsTablePrefix)) {
            statsTableName = TableUtils.getFullTableName(statsTablePrefix, pipelineVersion);
            updateEntityValueMapInContext(STATS_TABLE_NAMES, statsTableName, String.class);
        }
    }

    protected void initializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        entity = getEntity();

        TableRoleInCollection servingStore = entity.getServingStore();
        profileTablePrefix = entity.name() + "Profile";
        statsTablePrefix = entity.name() + "Stats";
        servingStoreTablePrefix = servingStore.name();
        servingStoreSortKey = servingStore.getPrimaryKey().name();

        if (entity.getBatchStore() != null) {
            String masterTableName = dataCollectionProxy.getTableName(customerSpace.toString(), entity.getBatchStore(),
                    inactive);
            if (StringUtils.isBlank(masterTableName)) {
                masterTableName = dataCollectionProxy.getTableName(customerSpace.toString(), entity.getBatchStore(),
                        active);
                if (StringUtils.isNotBlank(masterTableName)) {
                    log.info("Found the batch store in active version " + active + ": " + masterTableName);
                    tableFromActiveVersion = true;
                }
            } else {
                log.info("Found the batch store in inactive version " + inactive + ": " + masterTableName);
            }
            masterTable = metadataProxy.getTable(customerSpace.toString(), masterTableName);
            if (masterTable == null) {
                throw new IllegalStateException("Cannot find the master table in default collection");
            }
            double sizeInGb = ScalingUtils.getTableSizeInGb(yarnConfiguration, masterTable);
            int multiplier = ScalingUtils.getMultiplier(sizeInGb);
            if (multiplier > 1) {
                log.info("Set multiplier=" + multiplier + " base on master table size=" + sizeInGb + " gb.");
                scalingMultiplier = multiplier;
            }
        }
    }

    protected TransformationWorkflowConfiguration generateWorkflowConf() {
        PipelineTransformationRequest request = getTransformRequest();
        if (request == null) {
            return null;
        } else {
            return transformationProxy.getWorkflowConf(customerSpace.toString(), request, configuration.getPodId());
        }
    }

    private void upsertProfileTable(String profileTableName, TableRoleInCollection profileRole) {
        String customerSpace = configuration.getCustomerSpace().toString();
        Table profileTable = metadataProxy.getTable(customerSpace, profileTableName);
        if (profileTable == null) {
            throw new RuntimeException(
                    "Failed to find profile table " + profileTableName + " in customer " + customerSpace);
        }
        DataCollection.Version inactiveVersion = dataCollectionProxy.getInactiveVersion(customerSpace);
        dataCollectionProxy.upsertTable(customerSpace, profileTableName, profileRole, inactiveVersion);
        profileTable = dataCollectionProxy.getTable(customerSpace, profileRole, inactiveVersion);
        if (profileTable == null) {
            throw new IllegalStateException("Cannot find the upserted " + profileRole + " table in data collection.");
        }
    }

    protected void enrichTableSchema(Table servingStoreTable) {
    }

    Attribute copyMasterAttr(Map<String, Attribute> masterAttrs, Attribute attr0) {
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
        updateEntityValueMapInContext(entity, key, value, clz);
    }

    protected <V> void updateEntityValueMapInContext(BusinessEntity entity, String key, V value, Class<V> clz) {
        Map<BusinessEntity, V> entityValueMap = getMapObjectFromContext(key, BusinessEntity.class, clz);
        if (entityValueMap == null) {
            entityValueMap = new HashMap<>();
        }
        entityValueMap.put(entity, value);
        putObjectInContext(key, entityValueMap);
    }

    @Override
    protected BusinessEntity getEntity() {
        return configuration.getMainEntity();
    }

    protected abstract TableRoleInCollection profileTableRole();

    protected abstract PipelineTransformationRequest getTransformRequest();

}
