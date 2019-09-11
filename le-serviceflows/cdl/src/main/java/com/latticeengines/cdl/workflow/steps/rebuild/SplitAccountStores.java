package com.latticeengines.cdl.workflow.steps.rebuild;


import static com.latticeengines.domain.exposed.cache.CacheName.TableRoleMetadataCache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.domain.exposed.spark.common.MultiCopyConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.common.MultiCopyJob;

@Component(SplitAccountStores.BEAN_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SplitAccountStores extends RunSparkJob<ProcessAccountStepConfiguration, MultiCopyConfig> {

    private static final Logger log = LoggerFactory.getLogger(SplitAccountStores.class);

    static final String BEAN_NAME = "splitAccountStores";

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    private boolean shortCutMode = false;
    private DataCollection.Version inactive;
    private Table fullAccountTable;

    @Override
    protected Class<MultiCopyJob> getJobClz() {
        return MultiCopyJob.class;
    }

    @Override
    protected CustomerSpace parseCustomerSpace(ProcessAccountStepConfiguration stepConfiguration) {
        return stepConfiguration.getCustomerSpace();
    }

    @Override
    protected MultiCopyConfig configureJob(ProcessAccountStepConfiguration stepConfiguration) {
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        List<Table> tablesInCtx = getTableSummariesFromCtxKeys(customerSpace.toString(), Arrays.asList(
                ACCOUNT_EXPORT_TABLE_NAME, ACCOUNT_FEATURE_TABLE_NAME));
        shortCutMode = tablesInCtx.stream().noneMatch(Objects::isNull);
        if (shortCutMode) {
            log.info("Found account export and account features tables in context, going thru short-cut mode.");
            String accountExportTableName = tablesInCtx.get(0).getName();
            String accountFeaturesTableName = tablesInCtx.get(1).getName();
            dataCollectionProxy.upsertTable(customerSpace.toString(), accountExportTableName, //
                    TableRoleInCollection.AccountExport, inactive);
            dataCollectionProxy.upsertTable(customerSpace.toString(), accountFeaturesTableName, //
                    TableRoleInCollection.AccountFeatures, inactive);
            return null;
        } else {
            String fullAccountTableName = getStringValueFromContext(FULL_ACCOUNT_TABLE_NAME);
            if (StringUtils.isBlank(fullAccountTableName)) {
                throw new IllegalStateException("Cannot find the fully enriched account table");
            }
            fullAccountTable = metadataProxy.getTableSummary(customerSpace.toString(), fullAccountTableName);
            if (fullAccountTable == null) {
                throw new IllegalStateException("Cannot find the fully enriched account table in default collection");
            }

            CacheService cacheService = CacheServiceBase.getCacheService();
            cacheService.refreshKeysByPattern(customerSpace.getTenantId(), TableRoleMetadataCache);
            try {
                Thread.sleep(10000L);
            } catch (InterruptedException e) {
                log.warn("10 second sleep is interrupted.", e);
            }

            CopyConfig filterAccountExport = getExportCopyConfig();
            CopyConfig filterAccountFeatures = getFeaturesCopyConfig();
            MultiCopyConfig config = new MultiCopyConfig();
            config.setInput(Collections.singletonList(fullAccountTable.toHdfsDataUnit("FullAccount")));
            config.setCopyConfigs(Arrays.asList(filterAccountExport, filterAccountFeatures));
            config.setSpecialTarget(0, DataUnit.DataFormat.PARQUET);
            return config;
        }
    }

    private CopyConfig getFeaturesCopyConfig() {
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

        CopyConfig config = new CopyConfig();
        config.setSelectAttrs(retainAttrNames);
        return config;
    }

    private CopyConfig getExportCopyConfig() {
        List<String> retainAttrNames = servingStoreProxy
                .getDecoratedMetadata(customerSpace.toString(), BusinessEntity.Account, null,
                        inactive) //
                .filter(cm -> !AttrState.Inactive.equals(cm.getAttrState())) //
                .filter(cm -> !(Boolean.FALSE.equals(cm.getCanSegment()) //
                        && Boolean.FALSE.equals(cm.getCanEnrich()))) //
                .map(ColumnMetadata::getAttrName) //
                .collectList().block();
        if (retainAttrNames == null) {
            retainAttrNames = new ArrayList<>();
        }
        if (!retainAttrNames.contains(InterfaceName.AccountId.name())) {
            retainAttrNames.add(InterfaceName.AccountId.name());
        }

        CopyConfig config = new CopyConfig();
        config.setSelectAttrs(retainAttrNames);
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        if (shortCutMode) {
            return;
        }
        processAccountExportResult(result.getTargets().get(0));
        processAccountFeaturesResult(result.getTargets().get(1));
    }

    private void processAccountExportResult(HdfsDataUnit result) {
        String filteredTableName = NamingUtils.timestamp("AccountExport");
        Table filteredTable = toTable(filteredTableName, InterfaceName.AccountId.name(), result);
        metadataProxy.createTable(customerSpace.toString(), filteredTableName, filteredTable);
        dataCollectionProxy.upsertTable(customerSpace.toString(), filteredTableName, //
                TableRoleInCollection.AccountExport, inactive);
        exportToS3AndAddToContext(filteredTable, ACCOUNT_EXPORT_TABLE_NAME);
    }

    private void processAccountFeaturesResult(HdfsDataUnit result) {
        String filteredTableName = NamingUtils.timestamp("AccountFeatures");
        Table filteredTable = toTable(filteredTableName, InterfaceName.AccountId.name(), result);
        setAccountFeatureTableSchema(filteredTable);
        metadataProxy.createTable(customerSpace.toString(), filteredTableName, filteredTable);
        dataCollectionProxy.upsertTable(customerSpace.toString(), filteredTableName, //
                TableRoleInCollection.AccountFeatures, inactive);
        exportToS3AndAddToContext(filteredTable, ACCOUNT_FEATURE_TABLE_NAME);
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
    }

}
