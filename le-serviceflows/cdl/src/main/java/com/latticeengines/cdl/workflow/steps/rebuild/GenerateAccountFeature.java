package com.latticeengines.cdl.workflow.steps.rebuild;


import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_COPIER;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CopierConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;

@Component(GenerateAccountFeature.BEAN_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class GenerateAccountFeature extends ProfileStepBase<ProcessAccountStepConfiguration> {

    static final String BEAN_NAME = "generateAccountFeature";

    private static final Logger log = LoggerFactory.getLogger(GenerateAccountFeature.class);

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    private DataCollection.Version inactive;
    private String fullAccountTableName;
    private String accountFeaturesTablePrefix = "AccountFeatures";

    @Override
    protected BusinessEntity getEntity() {
        return BusinessEntity.Account;
    }

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        customerSpace = configuration.getCustomerSpace();
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        String accountFeatureTableName = getStringValueFromContext(ACCOUNT_FEATURE_TABLE_NAME);
        if (StringUtils.isNotBlank(accountFeatureTableName)) {
            Table accountFeatureTable = metadataProxy.getTable(customerSpace.toString(), accountFeatureTableName);
            if (accountFeatureTable != null) {
                log.info("Found account feature table in context, going thru short-cut mode.");
                dataCollectionProxy.upsertTable(customerSpace.toString(), accountFeatureTableName, //
                        TableRoleInCollection.AccountFeatures, inactive);
                return null;
            }
        }

        fullAccountTableName = getStringValueFromContext(FULL_ACCOUNT_TABLE_NAME);
        if (StringUtils.isBlank(fullAccountTableName)) {
            throw new IllegalStateException("Cannot find the fully enriched account table");
        }
        Table fullAccountTable = metadataProxy.getTable(customerSpace.toString(), fullAccountTableName);
        if (fullAccountTable == null) {
            throw new IllegalStateException("Cannot find the fully enriched account table in default collection");
        }
        long count = ScalingUtils.getTableCount(fullAccountTable);
        int multiplier = ScalingUtils.getMultiplier(count);
        if (multiplier > 1) {
            log.info("Set multiplier=" + multiplier + " base on fully enriched account table count=" + count);
            scalingMultiplier = multiplier;
        }

        PipelineTransformationRequest request = getTransformRequest();
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    @Override
    protected void onPostTransformationCompleted() {
        String accountFeatureTableName = TableUtils.getFullTableName(accountFeaturesTablePrefix, pipelineVersion);
        Table accountFeatureTable = metadataProxy.getTable(customerSpace.toString(), accountFeatureTableName);
        if (accountFeatureTable == null) {
            throw new RuntimeException("Cannot find generated account feature table " + accountFeatureTableName);
        }
        setAccountFeatureTableSchema(accountFeatureTable);
        dataCollectionProxy.upsertTable(customerSpace.toString(), accountFeatureTableName, TableRoleInCollection.AccountFeatures,
                inactive);
        accountFeatureTable = dataCollectionProxy.getTable(customerSpace.toString(), TableRoleInCollection.AccountFeatures,
                inactive);
        if (accountFeatureTable == null) {
            throw new IllegalStateException(
                    "Cannot find the upserted " + TableRoleInCollection.AccountFeatures + " table in data collection.");
        }
        exportToS3AndAddToContext(accountFeatureTableName, ACCOUNT_FEATURE_TABLE_NAME);
    }

    private PipelineTransformationRequest getTransformRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("AccountFeature");
        request.setSubmitter(customerSpace.getTenantId());
        request.setKeepTemp(false);
        request.setEnableSlack(false);

        TransformationStepConfig filter = filter();

        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(filter);
        request.setSteps(steps);
        return request;
    }

    private TransformationStepConfig filter() {
        TransformationStepConfig step = new TransformationStepConfig();
        addBaseTables(step, fullAccountTableName);
        step.setTransformer(TRANSFORMER_COPIER);
        List<String> retainAttrNames = servingStoreProxy //
                .getAllowedModelingAttrs(customerSpace.toString(), true, inactive) //
                .map(ColumnMetadata::getAttrName) //
                .collectList().block();
        if (retainAttrNames == null) {
            retainAttrNames = new ArrayList<>();
        }
        log.info(String.format("retainAttrNames from servingStore: %d", retainAttrNames.size()));
        if (!retainAttrNames.contains(InterfaceName.AccountId.name())) {
            retainAttrNames.add(InterfaceName.AccountId.name());
        }
        if (!retainAttrNames.contains(InterfaceName.LatticeAccountId.name())) {
            retainAttrNames.add(InterfaceName.LatticeAccountId.name());
        }

        CopierConfig conf = new CopierConfig();
        conf.setRetainAttrs(retainAttrNames);
        String confStr = appendEngineConf(conf, heavyEngineConfig());
        step.setConfiguration(confStr);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(accountFeaturesTablePrefix);
        step.setTargetTable(targetTable);

        return step;
    }

    private void setAccountFeatureTableSchema(Table table) {
        String dataCloudVersion = configuration.getDataCloudVersion();
        List<ColumnMetadata> amCols = columnMetadataProxy.columnSelection(ColumnSelection.Predefined.Model,
                dataCloudVersion);
        Map<String, ColumnMetadata> amColMap = new HashMap<>();
        amCols.forEach(cm -> amColMap.put(cm.getAttrName(), cm));
        List<Attribute> attrs = new ArrayList<>();
        table.getAttributes().forEach(attr0 -> {
            if (amColMap.containsKey(attr0.getName())) {
                ColumnMetadata cm = amColMap.get(attr0.getName());
                if (Category.ACCOUNT_ATTRIBUTES.equals(cm.getCategory())) {
                    attr0.setTags(Tag.INTERNAL);
                }
            }
            attrs.add(attr0);
        });
        table.setAttributes(attrs);
        metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);
    }

}
