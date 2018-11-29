package com.latticeengines.datacloud.match.service.impl;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;
import com.amazonaws.services.dynamodbv2.xspec.UpdateItemExpressionSpec;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.CDLConfigurationService;
import com.latticeengines.datacloud.match.service.CDLMatchVersionService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLMatchEnvironment;
import com.latticeengines.domain.exposed.security.Tenant;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.N;

@Component("cdlMatchVersionService")
public class CDLMatchVersionServiceImpl implements CDLMatchVersionService {

    /* constants */
    private static final String PREFIX = DataCloudConstants.CDL_PREFIX_VERSION;
    private static final String ATTR_PARTITION_KEY = DataCloudConstants.CDL_ATTR_PID;
    private static final String ATTR_VERSION = DataCloudConstants.CDL_ATTR_VERSION;
    private static final String DELIMITER = DataCloudConstants.CDL_DELIMITER;
    // treat null item as version 0 because dynamo treat it this way (ADD on null item will result in 1)
    private static final int DEFAULT_VERSION = 0;

    private final DynamoItemService dynamoItemService;
    private String tableName; // currently store both version in serving for faster lookup

    @Inject
    public CDLMatchVersionServiceImpl(
            DynamoItemService dynamoItemService, CDLConfigurationService cdlConfigurationService) {
        this.dynamoItemService = dynamoItemService;
        // NOTE this will not be changed at runtime
        this.tableName = cdlConfigurationService.getTableName(CDLMatchEnvironment.SERVING);
    }

    @Override
    public int getCurrentVersion(@NotNull CDLMatchEnvironment environment, @NotNull Tenant tenant) {
        PrimaryKey key = buildKey(environment, tenant);
        Item item = dynamoItemService.getItem(tableName, key);
        return item == null ? DEFAULT_VERSION : item.getInt(ATTR_VERSION);
    }

    @Override
    public int bumpVersion(@NotNull CDLMatchEnvironment environment, @NotNull Tenant tenant) {
        PrimaryKey key = buildKey(environment, tenant);
        UpdateItemExpressionSpec expressionSpec = new ExpressionSpecBuilder()
                .addUpdate(N(ATTR_VERSION).add(1)) // increase by one
                .buildForUpdate();
        UpdateItemSpec spec = new UpdateItemSpec()
                .withPrimaryKey(key)
                .withExpressionSpec(expressionSpec)
                .withReturnValues(ReturnValue.UPDATED_NEW); // get the updated version back
        UpdateItemOutcome result = dynamoItemService.update(tableName, spec);
        Preconditions.checkNotNull(result);
        Preconditions.checkNotNull(result.getItem());
        return result.getItem().getInt(ATTR_VERSION);
    }

    @Override
    public void clearVersion(@NotNull CDLMatchEnvironment environment, @NotNull Tenant tenant) {
        PrimaryKey key = buildKey(environment, tenant);
        dynamoItemService.delete(tableName, new DeleteItemSpec().withPrimaryKey(key));
    }

    @VisibleForTesting
    void setTableName(@NotNull String tableName) {
        Preconditions.checkNotNull(tableName);
        this.tableName = tableName;
    }

    /*
     * Build dynamo key, schema:
     * - Partition Key: <PREFIX>_<ENV>_<TENANT_PID>   E.g., "VERSION_STAGING_123"
     */
    private PrimaryKey buildKey(@NotNull CDLMatchEnvironment environment, @NotNull Tenant tenant) {
        Preconditions.checkNotNull(environment);
        Preconditions.checkNotNull(tenant);
        Preconditions.checkNotNull(tenant.getPid());

        String partitionKey = String.join(DELIMITER, PREFIX, environment.name(), tenant.getPid().toString());
        return new PrimaryKey(ATTR_PARTITION_KEY, partitionKey);
    }
}
