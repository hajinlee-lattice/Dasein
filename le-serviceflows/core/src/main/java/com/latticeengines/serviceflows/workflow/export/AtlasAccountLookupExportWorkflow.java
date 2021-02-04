package com.latticeengines.serviceflows.workflow.export;

import static com.latticeengines.domain.exposed.admin.LatticeFeatureFlag.ENABLE_ACCOUNT360;
import static com.latticeengines.domain.exposed.admin.LatticeModule.TalkingPoint;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.AtlasAccountLookupExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

@Component("atlasAccountLookupExportWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class AtlasAccountLookupExportWorkflow extends BaseExportToDynamo<AtlasAccountLookupExportStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(AtlasAccountLookupExportWorkflow.class);

    private static List<String> lookupIds;

    private static Integer curVersion;

    private static Long expTime;

    @Value("${eai.export.dynamo.accountlookup.signature}")
    private String accountLookupSignature;

    @Value("${cdl.processAnalyze.skip.dynamo.publication}")
    private boolean skipPublishDynamo;

    private String targetLookupSignature;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private BatonService batonService;

    @Override
    public void execute() {
        log.info("Using account changelist to populate account lookup.");
        if (!shouldPublishDynamo()) {
            log.info("Skip updating account lookup dynamo");
            return;
        }
        lookupIds = getLookupIds();
        if (CollectionUtils.isEmpty(lookupIds)) {
            log.info("No lookup IDs in new account batch store");
            return;
        }
        if (needPublishAccountLookup(configuration.getCustomerSpace().getTenantId())) {
            putObjectInContext(NEED_PUBLISH_ACCOUNT_LOOKUP, Boolean.TRUE);
            log.info("No account lookup data unit found. Account lookup will be published after PA success.");
            return;
        }
        initProps();
        List<DynamoExportConfig> configs = getExportConfigs();
        if (CollectionUtils.isEmpty(configs)) {
            log.info("No change list needs to populate, skip execution.");
            return;
        }
        log.info("Going to populate changelist to account lookup: " + configs);
        markTablesInDataCollection(configs);
        log.info("Using dynamo table {}",
                String.format("%s_%s", ACCOUNT_LOOKUP_DATA_UNIT_NAME, targetLookupSignature));
        List<AccountLookupExporter> exporters = getLookupExporters(configs);
        int threadPoolSize = Math.min(2, configs.size());
        ExecutorService executors = ThreadPoolUtils.getFixedSizeThreadPool("dynamo-export", threadPoolSize);
        ThreadPoolUtils.runInParallel(executors, exporters, (int) TimeUnit.DAYS.toMinutes(2), 10);
    }

    private boolean needPublishAccountLookup(String tenantId) {
        DynamoDataUnit unit = (DynamoDataUnit) dataUnitProxy.getByNameAndType(tenantId, ACCOUNT_LOOKUP_DATA_UNIT_NAME,
                DataUnit.StorageType.Dynamo);
        return unit == null;
    }

    private List<String> getLookupIds() {
        DataCollection.Version inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        List<String> ids = servingStoreProxy
                .getDecoratedMetadata(configuration.getCustomerSpace().toString(), BusinessEntity.Account,
                        Collections.singletonList(ColumnSelection.Predefined.LookupId), inactive)
                .map(ColumnMetadata::getAttrName).collectList().block();
        if (CollectionUtils.isEmpty(ids)) {
            return new ArrayList<>();
        } else {
            log.info("Found lookup IDs in new account batch store: {}", ids);
            return ids;
        }
    }

    private void initProps() {
        DynamoDataUnit unit = (DynamoDataUnit) dataUnitProxy.getByNameAndType(
                configuration.getCustomerSpace().toString(), ACCOUNT_LOOKUP_DATA_UNIT_NAME,
                DataUnit.StorageType.Dynamo);
        curVersion = unit.getVersion() == null ? 0 : unit.getVersion();
        expTime = Instant.now().plus(30, ChronoUnit.DAYS).getEpochSecond();
        targetLookupSignature = StringUtils.isBlank(unit.getSignature()) ? accountLookupSignature : unit.getSignature();
    }

    private List<AccountLookupExporter> getLookupExporters(List<DynamoExportConfig> configs) {
        return configs.stream().map(AccountLookupExporter::new).collect(Collectors.toList());
    }

    private boolean shouldPublishDynamo() {
        boolean enableTp = batonService.hasModule(configuration.getCustomerSpace(), TalkingPoint);
        boolean hasAccount360 = batonService.isEnabled(configuration.getCustomerSpace(), ENABLE_ACCOUNT360);
        return !skipPublishDynamo && (hasAccount360 || enableTp);
    }

    private void markTablesInDataCollection(List<DynamoExportConfig> configs) {
        DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        List<String> tableNames = configs.stream().map(DynamoExportConfig::getTableName).collect(Collectors.toList());
        status.setAccountLookupSource(tableNames);
        putObjectInContext(CDL_COLLECTION_STATUS, status);
    }

    protected class AccountLookupExporter extends Exporter {
        AccountLookupExporter(DynamoExportConfig config) {
            super(config);
        }

        @Override
        protected Map<String, String> getProperties(String recordClass, String recordType, String tenantId) {
            Map<String, String> properties = new HashMap<>();
            properties.put(HdfsToDynamoConfiguration.CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED,
                    CipherUtils.encrypt(awsAccessKey));
            properties.put(HdfsToDynamoConfiguration.CONFIG_AWS_SECRET_KEY_ENCRYPTED,
                    CipherUtils.encrypt(awsSecretKey));
            properties.put(HdfsToDynamoConfiguration.CONFIG_AWS_REGION, awsRegion);
            properties.put(HdfsToDynamoConfiguration.CONFIG_ATLAS_LOOKUP_IDS, String.join(",", lookupIds));
            properties.put(HdfsToDynamoConfiguration.CONFIG_EXPORT_VERSION, curVersion.toString());
            properties.put(HdfsToDynamoConfiguration.CONFIG_TABLE_NAME,
                    String.format("%s_%s", ACCOUNT_LOOKUP_DATA_UNIT_NAME, targetLookupSignature));
            properties.put(HdfsToDynamoConfiguration.CONFIG_ATLAS_LOOKUP_TTL, expTime.toString());
            properties.put(HdfsToDynamoConfiguration.CONFIG_EXPORT_TYPE,
                    AtlasAccountLookupExportStepConfiguration.NAME);
            properties.put(ExportProperty.NUM_MAPPERS, String.valueOf(numMappers));

            return properties;
        }
    }
}
