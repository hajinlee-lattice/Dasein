package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AccountLookup;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.AccountLookupToDynamoStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.serviceflows.workflow.export.BaseExportToDynamo;

@Component("accountLookupToDynamoStep")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class AccountLookupToDynamoStep extends BaseExportToDynamo<AccountLookupToDynamoStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(AccountLookupToDynamoStep.class);

    private static List<String> lookupIds;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Override
    public void execute() {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        lookupIds = getLookupIds();
        if (CollectionUtils.isEmpty(lookupIds)) {
            throw new UnsupportedOperationException(
                    String.format("No attributes in LookupId group for tenant %s", customerSpace.getTenantId()));
        }
        Table lookupTable = dataCollectionProxy.getTable(customerSpace.toString(), AccountLookup);
        if (lookupTable == null) {
            throw new UnsupportedOperationException(
                    String.format("Failed to retrieve lookup table for tenant %s", customerSpace.getTenantId()));
        }
        String lookupTablePath = lookupTable.getExtracts().get(0).getPath();
        log.info("Retrieved lookup table {} with path {}", lookupTable.getName(), lookupTablePath);
        HdfsToDynamoConfiguration eaiConfig = generateEaiConfig(lookupTable.getName(), lookupTablePath);
        RetryTemplate retry = RetryUtils.getExponentialBackoffRetryTemplate(3, 5000, 2.0, null);
        AppSubmission appSubmission = retry.execute(context -> {
            if (context.getRetryCount() > 0) {
                log.info("(Attempt=" + (context.getRetryCount() + 1) + ") submitting eai job.");
            }
            return eaiProxy.submitEaiJob(eaiConfig);
        });
        String appId = appSubmission.getApplicationIds().get(0);
        JobStatus jobStatus = jobService.waitFinalJobStatus(appId, ONE_DAY.intValue());

        if (!FinalApplicationStatus.SUCCEEDED.equals(jobStatus.getStatus())) {
            throw new RuntimeException("Yarn application " + appId + " did not finish in SUCCEEDED status, but " //
                    + jobStatus.getStatus() + " instead.");
        }
        registerDataUnit();
    }

    private List<String> getLookupIds() {
        List<String> ids = servingStoreProxy
                .getDecoratedMetadata(configuration.getCustomerSpace().toString(), BusinessEntity.Account,
                        Collections.singletonList(ColumnSelection.Predefined.LookupId))
                .map(ColumnMetadata::getAttrName).collectList().block();
        if (CollectionUtils.isEmpty(ids)) {
            return new ArrayList<>();
        } else {
            log.info("Found lookup IDs in new account batch store: {}", ids);
            return ids;
        }
    }

    private HdfsToDynamoConfiguration generateEaiConfig(String lookupTableName, String lookupTablePath) {
        HdfsToDynamoConfiguration eaiConfig = new HdfsToDynamoConfiguration();

        eaiConfig.setName("AccountLookupToDynamo_" + lookupTableName);
        eaiConfig.setCustomerSpace(configuration.getCustomerSpace());
        eaiConfig.setExportDestination(ExportDestination.DYNAMO);
        eaiConfig.setExportFormat(ExportFormat.AVRO);
        eaiConfig.setExportInputPath(lookupTablePath);
        eaiConfig.setUsingDisplayName(false);
        eaiConfig.setExportTargetPath("/tmp/path");
        eaiConfig.setProperties(getProperties());

        return eaiConfig;
    }

    private Map<String, String> getProperties() {
        Map<String, String> properties = new HashMap<>();

        String expTime = Long.toString(Instant.now().plus(30, ChronoUnit.DAYS).getEpochSecond());

        properties.put(HdfsToDynamoConfiguration.CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED, CipherUtils.encrypt(awsAccessKey));
        properties.put(HdfsToDynamoConfiguration.CONFIG_AWS_SECRET_KEY_ENCRYPTED, CipherUtils.encrypt(awsSecretKey));
        properties.put(HdfsToDynamoConfiguration.CONFIG_AWS_REGION, awsRegion);
        properties.put(HdfsToDynamoConfiguration.CONFIG_EXPORT_VERSION, getTargetVersion());
        properties.put(HdfsToDynamoConfiguration.CONFIG_TABLE_NAME,
                String.format("%s_%s", ACCOUNT_LOOKUP_DATA_UNIT_NAME, configuration.getDynamoSignature()));
        properties.put(HdfsToDynamoConfiguration.CONFIG_ATLAS_LOOKUP_IDS, String.join(",", lookupIds));
        properties.put(HdfsToDynamoConfiguration.CONFIG_ATLAS_LOOKUP_TTL, expTime);
        properties.put(HdfsToDynamoConfiguration.CONFIG_ATLAS_TENANT, configuration.getCustomerSpace().getTenantId());
        properties.put(HdfsToDynamoConfiguration.CONFIG_EXPORT_TYPE, AccountLookupToDynamoStepConfiguration.NAME);
        properties.put(ExportProperty.NUM_MAPPERS, String.valueOf(numMappers));

        log.info("Using dynamo AccountLookup table: {}", properties.get(HdfsToDynamoConfiguration.CONFIG_TABLE_NAME));
        log.info("Publishing version {}", properties.get(HdfsToDynamoConfiguration.CONFIG_EXPORT_VERSION));
        return properties;
    }

    private String getTargetVersion() {
        DynamoDataUnit unit = (DynamoDataUnit) dataUnitProxy.getByNameAndType(
                configuration.getCustomerSpace().toString(), ACCOUNT_LOOKUP_DATA_UNIT_NAME,
                DataUnit.StorageType.Dynamo);
        return Integer.toString(unit == null ? 0 : unit.getVersion() + 1);
    }

    private void registerDataUnit() {
        String customerSpace = configuration.getCustomerSpace().getTenantId();
        DynamoDataUnit unit = (DynamoDataUnit) dataUnitProxy.getByNameAndType(customerSpace,
                ACCOUNT_LOOKUP_DATA_UNIT_NAME, DataUnit.StorageType.Dynamo);
        if (unit == null) {
            unit = initUnit(customerSpace);
        } else {
            unit.setVersion(unit.getVersion() + 1);
        }
        dataUnitProxy.create(customerSpace, unit);
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
