package com.latticeengines.serviceflows.workflow.export;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
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

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Override
    public void execute() {
        log.info("Using account changelist to populate account lookup.");
        lookupIds = getLookupIds();
        if (CollectionUtils.isEmpty(lookupIds)) {
            log.info("No lookup IDs in new account batch store");
            return;
        }
        curVersion = getCurrentVersion();
        expTime = Instant.now().plus(30, ChronoUnit.DAYS).getEpochSecond();
        List<DynamoExportConfig> configs = getExportConfigs();
        if (CollectionUtils.isEmpty(configs)) {
            log.info("No change list needs to populate, skip execution.");
            return;
        }
        log.info("Going to populate changelist to account lookup: " + configs);
        log.info("Using dynamo table {}", String.format("%s_%s", ACCOUNT_LOOKUP_DATA_UNIT_NAME, accountLookupSignature));
        List<AccountLookupExporter> exporters = getLookupExporters(configs);
        int threadPoolSize = Math.min(2, configs.size());
        ExecutorService executors = ThreadPoolUtils.getFixedSizeThreadPool("dynamo-export", threadPoolSize);
        ThreadPoolUtils.runInParallel(executors, exporters, (int) TimeUnit.DAYS.toMinutes(2), 10);
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

    private int getCurrentVersion() {
        DynamoDataUnit unit = (DynamoDataUnit) dataUnitProxy.getByNameAndType(
                configuration.getCustomerSpace().toString(), ACCOUNT_LOOKUP_DATA_UNIT_NAME,
                DataUnit.StorageType.Dynamo);
        if (unit == null || unit.getVersion() == null) {
            return 0;
        }
        return unit.getVersion();
    }

    private List<AccountLookupExporter> getLookupExporters(List<DynamoExportConfig> configs) {
        return configs.stream().map(AccountLookupExporter::new).collect(Collectors.toList());
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
            properties.put(HdfsToDynamoConfiguration.CONFIG_CURRENT_VERSION, curVersion.toString());
            properties.put(HdfsToDynamoConfiguration.CONFIG_TABLE_NAME, String.format("%s_%s", ACCOUNT_LOOKUP_DATA_UNIT_NAME, accountLookupSignature));
            properties.put(HdfsToDynamoConfiguration.CONFIG_ATLAS_LOOKUP_TTL, expTime.toString());
            properties.put(ExportProperty.NUM_MAPPERS, String.valueOf(numMappers));

            return properties;
        }

        @Override
        void registerDataUnit() {
            String customerSpace = configuration.getCustomerSpace().toString();
            DynamoDataUnit unit = (DynamoDataUnit) dataUnitProxy.getByNameAndType(customerSpace,
                    ACCOUNT_LOOKUP_DATA_UNIT_NAME, DataUnit.StorageType.Dynamo);
            if (unit == null) {
                dataUnitProxy.create(customerSpace, initUnit(customerSpace));
            }
        }

        private DynamoDataUnit initUnit(String customerSpace) {
            DynamoDataUnit unit = new DynamoDataUnit();
            unit.setTenant(customerSpace);
            unit.setName(ACCOUNT_LOOKUP_DATA_UNIT_NAME);
            unit.setVersion(0);
            unit.setPartitionKey(InterfaceName.AtlasLookupKey.name());
            return unit;
        }
    }
}
