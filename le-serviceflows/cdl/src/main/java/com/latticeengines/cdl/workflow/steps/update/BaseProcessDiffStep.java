package com.latticeengines.cdl.workflow.steps.update;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.RedshiftExportConfig;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

public abstract class BaseProcessDiffStep<T extends BaseProcessEntityStepConfiguration>
        extends BaseTransformWrapperStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseProcessDiffStep.class);

    protected CustomerSpace customerSpace;
    protected DataCollection.Version active;
    protected DataCollection.Version inactive;

    protected BusinessEntity entity;

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
    }

    protected <V> void updateEntityValueMapInContext(BusinessEntity entity, String key, V value, Class<V> clz) {
        Map<BusinessEntity, V> entityValueMap = getMapObjectFromContext(key, BusinessEntity.class, clz);
        if (entityValueMap == null) {
            entityValueMap = new HashMap<>();
        }
        entityValueMap.put(entity, value);
        putObjectInContext(key, entityValueMap);
    }

    protected void initializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
    }

    private TransformationWorkflowConfiguration generateWorkflowConf() {
        PipelineTransformationRequest request = getTransformRequest();
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    protected abstract PipelineTransformationRequest getTransformRequest();

    protected String renameServingStoreTable(BusinessEntity servingEntity, Table servingStoreTable) {
        String prefix = String.join("_", customerSpace.getTenantId(), servingEntity.name());
        String goodName = NamingUtils.timestamp(prefix);
        log.info("Renaming table " + servingStoreTable.getName() + " to " + goodName);
        metadataProxy.updateTable(customerSpace.toString(), goodName, servingStoreTable);
        servingStoreTable.setName(goodName);
        return goodName;
    }

    protected RedshiftExportConfig exportTableRole(String tableName, TableRoleInCollection tableRole) {
        String targetTableName = dataCollectionProxy.getTableName(configuration.getCustomerSpace().toString(),
                tableRole, inactive);
        if (StringUtils.isBlank(targetTableName)) {
            throw new IllegalStateException("Cannot find serving store table " + tableRole + " in " + inactive +" for redshift upsert.");
        }

        String distKey = tableRole.getPrimaryKey().name();
        List<String> sortKeys = new ArrayList<>(tableRole.getForeignKeysAsStringList());
        if (!sortKeys.contains(tableRole.getPrimaryKey().name())) {
            sortKeys.add(tableRole.getPrimaryKey().name());
        }

        RedshiftExportConfig config = new RedshiftExportConfig();
        config.setTableName(targetTableName);
        config.setDistKey(distKey);
        config.setSortKeys(sortKeys);
        config.setInputPath(getInputPath(tableName) + "/*.avro");
        config.setUpdateMode(true);
        return config;
    }

    private String getInputPath(String tableName) {
        Table table = metadataProxy.getTable(configuration.getCustomerSpace().toString(), tableName);
        if (table == null) {
            throw new IllegalArgumentException("Cannot find table named " + tableName);
        }
        List<Extract> extracts = table.getExtracts();
        if (CollectionUtils.isEmpty(extracts) || extracts.size() != 1) {
            throw new IllegalArgumentException("Table " + tableName + " does not have single extract");
        }
        Extract extract = extracts.get(0);
        String path = extract.getPath();
        if (path.endsWith(".avro") || path.endsWith("/")) {
            path = path.substring(0, path.lastIndexOf("/"));
        }
        return path;
    }

}
