package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.cache.CacheName.TableRoleMetadataCache;

import java.util.ArrayList;
import java.util.Collections;
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
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.common.CopyJob;


@Component(FilterAccountFeature.BEAN_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FilterAccountFeature extends RunSparkJob<ProcessAccountStepConfiguration, CopyConfig> {

    private static final Logger log = LoggerFactory.getLogger(FilterAccountFeature.class);

    static final String BEAN_NAME = "filterAccountFeature";

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private boolean shortCutMode = false;
    private DataCollection.Version inactive;

    @Override
    protected Class<CopyJob> getJobClz() {
        return CopyJob.class;
    }

    @Override
    protected CopyConfig configureJob(ProcessAccountStepConfiguration stepConfiguration) {
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        Table accFeatureTableInCtx = getTableSummaryFromKey(customerSpace.toString(), ACCOUNT_FEATURE_TABLE_NAME);
        shortCutMode = accFeatureTableInCtx != null;

        if (shortCutMode) {
            log.info("Found account feature table in context, going thru short-cut mode.");
            String accountFeatureTableName = accFeatureTableInCtx.getName();
            dataCollectionProxy.upsertTable(customerSpace.toString(), accountFeatureTableName, //
                    TableRoleInCollection.AccountFeatures, inactive);
            return null;
        } else {
            String fullAccountTableName = getStringValueFromContext(FULL_ACCOUNT_TABLE_NAME);
            if (StringUtils.isBlank(fullAccountTableName)) {
                throw new IllegalStateException("Cannot find the fully enriched account table");
            }
            Table fullAccountTable = metadataProxy.getTable(customerSpace.toString(), fullAccountTableName);
            if (fullAccountTable == null) {
                throw new IllegalStateException("Cannot find the fully enriched account table in default collection");
            }

            CopyConfig config = new CopyConfig();
            config.setInput(Collections.singletonList(fullAccountTable.toHdfsDataUnit("FullAccount")));
            config.setSelectAttrs(getRetrainAttrs());
            return config;
        }
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        if (shortCutMode) {
            return;
        }
        String filteredTableName = NamingUtils.timestamp("AccountFeatures");
        Table filteredTable = toTable(filteredTableName, InterfaceName.AccountId.name(), result.getTargets().get(0));
        setAccountFeatureTableSchema(filteredTable);
        metadataProxy.createTable(customerSpace.toString(), filteredTableName, filteredTable);
        dataCollectionProxy.upsertTable(customerSpace.toString(), filteredTableName, //
                TableRoleInCollection.AccountFeatures, inactive);
        exportToS3AndAddToContext(filteredTable, ACCOUNT_FEATURE_TABLE_NAME);
    }

    private List<String> getRetrainAttrs() {
        CacheService cacheService = CacheServiceBase.getCacheService();
        cacheService.refreshKeysByPattern(customerSpace.getTenantId(), TableRoleMetadataCache);
        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            log.warn("10 second sleep is interrupted.", e);
        }
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
        return retainAttrNames;
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

    @Override
    protected CustomerSpace parseCustomerSpace(ProcessAccountStepConfiguration stepConfiguration) {
        return stepConfiguration.getCustomerSpace();
    }

}

