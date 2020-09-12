package com.latticeengines.cdl.workflow.steps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.rebuild.ProfileStepBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.ProductMapperConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicy;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicyTimeUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.RetentionPolicyUtil;
import com.latticeengines.domain.exposed.util.TableUtils;

@Component
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RollupProductStep extends ProfileStepBase<ProcessTransactionStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(RollupProductStep.class);

    private static final String ROLLUP_PRODUCT_TABLE_PREFIX_FMT = ROLLUP_PRODUCT_TABLE + "_%s"; // tenantId

    private DataCollection.Version inactive;
    private DataCollection.Version active;

    private String rollupProductTablePrefix;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initConfiguration();
        Table rollupProductTable = getTableSummaryFromKey(customerSpace.toString(), ROLLUP_PRODUCT_TABLE);
        if (rollupProductTable != null && tableIsInHdfs(rollupProductTable, false)) {
            log.info("Found rollup product table {} in context, going through shortcut mode", rollupProductTable.getName());
            saveRollupTable(rollupProductTable.getName());
            return null;
        }

        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("RollupProductStep");
        request.setSubmitter(customerSpace.getTenantId());
        request.setKeepTemp(false);
        request.setEnableSlack(false);
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(rollupProduct(getTable(TableRoleInCollection.ConsolidatedProduct)));
        request.setSteps(steps);
        return transformationProxy.getWorkflowConf(customerSpace.toString(), request, configuration.getPodId());
    }

    @Override
    protected void onPostTransformationCompleted() {
        String tableName = TableUtils.getFullTableName(rollupProductTablePrefix, pipelineVersion);
        log.info("Saving rollup product table {} for 3 days.", tableName);
        saveRollupTable(tableName);
        exportToS3AndAddToContext(tableName, ROLLUP_PRODUCT_TABLE);
    }

    private void saveRollupTable(String tableName) {
        RetentionPolicy retentionPolicy = RetentionPolicyUtil.toRetentionPolicy(3, RetentionPolicyTimeUnit.DAY);
        metadataProxy.updateDataTablePolicy(customerSpace.toString(), tableName, retentionPolicy);
    }

    private void initConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
    }

    private TransformationStepConfig rollupProduct(Table productTable) {
        TransformationStepConfig step = new TransformationStepConfig();
        Table rawTransactionTable = getTable(TableRoleInCollection.ConsolidatedRawTransaction);
        step.setTransformer(DataCloudConstants.PRODUCT_MAPPER);
        String tableSourceName = "CustomerUniverse";
        String rawTxnSourceTableName = rawTransactionTable.getName();
        String productTableName = productTable.getName();
        List<String> baseSources = Arrays.asList(tableSourceName, productTableName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, new SourceTable(rawTxnSourceTableName, customerSpace));
        baseTables.put(productTableName, new SourceTable(productTableName, customerSpace));
        step.setBaseTables(baseTables);
        rollupProductTablePrefix = String.format(ROLLUP_PRODUCT_TABLE_PREFIX_FMT, customerSpace.getTenantId());
        setTargetTable(step, rollupProductTablePrefix);

        ProductMapperConfig config = new ProductMapperConfig();
        config.setProductField(InterfaceName.ProductId.name());
        config.setProductTypeField(InterfaceName.ProductType.name());

        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private Table getTable(TableRoleInCollection role) {
        DataCollection.Version version = inactive;
        Table table = dataCollectionProxy.getTable(customerSpace.toString(), role, version);
        if (table == null) {
            log.info("Did not find {} in inactive version {}.", role.name(), inactive);
            version = active;
            table = dataCollectionProxy.getTable(customerSpace.toString(), role, version);
            if (table == null) {
                throw new IllegalStateException(String.format("Cannot find %s in both versions", role.name()));
            }
        }
        log.info("Found {} {} from version {}", role.name(), table.getName(), version);
        return table;
    }

    @Override
    protected BusinessEntity getEntity() {
        return BusinessEntity.PeriodTransaction;
    }
}
