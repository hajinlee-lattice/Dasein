package com.latticeengines.cdl.workflow.steps.process;

import static com.latticeengines.domain.exposed.metadata.DataCollectionArtifact.Status.GENERATING;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateIntentAlertArtifactsStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.GenerateIntentAlertArtifactsConfig;
import com.latticeengines.domain.exposed.spark.common.ConvertToCSVConfig;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.BaseSparkStep;
import com.latticeengines.spark.exposed.job.cdl.GenerateIntentAlertArtifactsJob;
import com.latticeengines.spark.exposed.job.common.ConvertToCSVJob;

@Component(GenerateIntentAlertArtifacts.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class GenerateIntentAlertArtifacts extends BaseSparkStep<GenerateIntentAlertArtifactsStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(GenerateIntentAlertArtifacts.class);

    static final String BEAN_NAME = "generateIntentAlertArtifacts";

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private ActivityStoreProxy activityStoreProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    private DataCollection.Version activeVersion;

    private DimensionMetadata dimensionMetadata;

    private String intentAlertVersion = null;
    private String streamId = null;

    private Table latticeAccountTable = null;
    private Table rawStreamTable = null;
    private Table ibmTable = null;
    private Table bsbmTable = null;

    private static final List<String> SELECTED_ATTRIBUTES = ImmutableList.of("ModelName", "Stage", "LDC_Name",
            "LDC_City", "STATE_PROVINCE_ABBR", "LDC_Country", "LDC_PrimaryIndustry", "LE_REVENUE_RANGE", "LDC_DUNS");

    @Override
    public void execute() {
        preExecution();

        SparkJobResult result = generateIntentAlertArtifacts();

        postExecution(result);
    }

    private void preExecution() {
        customerSpace = configuration.getCustomerSpace();
        activeVersion = dataCollectionProxy.getActiveVersion(customerSpace.toString());

        // Find out steam name and id
        List<AtlasStream> streams = activityStoreProxy.getStreams(customerSpace.toString());
        // Loop through streams to find the stream name for DnbIntentData type
        AtlasStream intentStream = streams.stream()
                .filter(stream -> (stream.getStreamType() == AtlasStream.StreamType.DnbIntentData)).findFirst().get();
        String streamName = intentStream.getName();
        streamId = intentStream.getStreamId();

        // Get dimensionMetadata for model names and ids
        Map<String, DimensionMetadata> dimensionMetadataMap = new HashMap<>();
        dimensionMetadataMap = activityStoreProxy.getDimensionMetadataInStream(customerSpace.toString(), streamName,
                null);
        if (MapUtils.isEmpty(dimensionMetadataMap)) {
            throw new RuntimeException(
                    String.format("Fail to get dimensionMetadata for DnbIntentData stream {}", streamName));
        } else {
            log.info("Stream {} configured to merge dimension metadata. Found active metadata {}", streamName,
                    JsonUtils.serialize(dimensionMetadataMap));
        }
        dimensionMetadata = dimensionMetadataMap.get(InterfaceName.ModelNameId.name());

        // Get LatticeAccount, RawStream and MetricsGroup tables for active version
        latticeAccountTable = dataCollectionProxy.getTable(customerSpace.toString(),
                TableRoleInCollection.LatticeAccount, activeVersion);
        if (latticeAccountTable == null) {
            log.error("Active LatticeAccount table doesn't exist");
            throw new RuntimeException("GenerateIntentAlertArtifacts failed due to no active LatticeAccount table");
        }

        // Get raw stream table
        String rawStreamTableName = dataCollectionProxy.getTableNamesWithSignatures(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedActivityStream, activeVersion, Collections.singletonList(streamId))
                .get(streamId);
        rawStreamTable = metadataProxy.getTable(customerSpace.toString(), rawStreamTableName);
        if (rawStreamTable == null) {
            log.error("Active RawStream table doesn't exist");
            throw new RuntimeException("GenerateIntentAlertArtifacts failed due to no active RawStream table");
        }

        // Loop through the metricGroup list to find the targeted groupIds
        String buyingStageGroupId = null;
        String hasIntentTimerangeGroupId = null;
        List<ActivityMetricsGroup> metricsGroups = activityStoreProxy.findGroupsByTenant(customerSpace.toString());
        for (ActivityMetricsGroup group : metricsGroups) {
            if (group.getCategory() == Category.DNBINTENTDATA_PROFILE) {
                String targetAttr = group.getAggregation().getTargetAttribute();
                if (StringUtils.equalsIgnoreCase(targetAttr, InterfaceName.BuyingScore.name())) {
                    buyingStageGroupId = group.getGroupId();
                } else if (StringUtils.equalsIgnoreCase(targetAttr, InterfaceName.HasIntent.name())) {
                    Set<List<Integer>> paraSet = group.getActivityTimeRange().getParamSet();
                    if (paraSet != null && paraSet.size() > 1) {
                        hasIntentTimerangeGroupId = group.getGroupId();
                    }
                }
            }
        }
        if (buyingStageGroupId == null || hasIntentTimerangeGroupId == null) {
            throw new RuntimeException("GenerateIntentAlertArtifacts failed due to missing metricGroup");
        }
        log.info("buyingStageGroupId: {}, hasIntentTimerangeGroupId: {}", buyingStageGroupId,
                hasIntentTimerangeGroupId);
        // Find the corresponding MetricsGroup table using groupId
        List<String> metricsGroupsignatures = Arrays.asList(hasIntentTimerangeGroupId, buyingStageGroupId);
        Map<String, String> tableNamesMap = dataCollectionProxy.getTableNamesWithSignatures(customerSpace.toString(),
                TableRoleInCollection.MetricsGroup, activeVersion, metricsGroupsignatures);
        ibmTable = metadataProxy.getTable(customerSpace.toString(), tableNamesMap.get(hasIntentTimerangeGroupId));
        bsbmTable = metadataProxy.getTable(customerSpace.toString(), tableNamesMap.get(buyingStageGroupId));
        if (ibmTable == null || bsbmTable == null) {
            log.error("Missing DNBIntent MetricsGroup related tables");
            throw new RuntimeException(
                    "GenerateIntentAlertArtifacts failed due to missing DNBIntent MetricsGroup related tables");
        }
    }

    private SparkJobResult generateIntentAlertArtifacts() {
        List<DataUnit> inputs = new ArrayList<>();
        inputs.add(toDataUnit(latticeAccountTable, "LatticeAccount"));
        // Raw stream is partitioned with StreamDateId, need to use
        // partitionedToHdfsDataUnit to get the DataUnit
        inputs.add(rawStreamTable.partitionedToHdfsDataUnit("RawStream",
                Collections.singletonList(InterfaceName.StreamDateId.name())));
        inputs.add(toDataUnit(ibmTable, "MetricsGroup_ibm"));
        inputs.add(toDataUnit(bsbmTable, "MetricsGroup_bsbm"));

        GenerateIntentAlertArtifactsConfig config = new GenerateIntentAlertArtifactsConfig();
        config.setDimensionMetadata(dimensionMetadata);
        config.setSelectedAttributes(SELECTED_ATTRIBUTES);
        config.setInput(inputs);
        log.info("Generating intent alerts, spark config = {}", JsonUtils.serialize(config));

        SparkJobResult intentAlertResult = runSparkJob(GenerateIntentAlertArtifactsJob.class, config);
        return intentAlertResult;
    }

    protected void postExecution(SparkJobResult result) {
        List<HdfsDataUnit> outputs = result.getTargets();
        Preconditions.checkArgument(CollectionUtils.size(outputs) == 2,
                String.format("Activity alert spark job should output two tables "
                        + "(new accounts shown intent this week, all accounts shown intent this week), got %d instead",
                        CollectionUtils.size(outputs)));

        // create artifacts for two output (new accounts shown intent this week, all
        // accounts shown intent this week)
        DataCollectionArtifact newAccounts = getOrCreateArtifact(DataCollectionArtifact.CURRENTWEEK_INTENT_NEWACCOUNTS);
        DataCollectionArtifact allAccounts = getOrCreateArtifact(DataCollectionArtifact.CURRENTWEEK_INTENT_ALLACCOUNTS);

        // Persist new accounts for current week into tables
        String newAccountsTableName = customerSpace.getTenantId() + "_"
                + NamingUtils.timestamp(DataCollectionArtifact.CURRENTWEEK_INTENT_NEWACCOUNTS);
        Table newAccountsTable = toTable(newAccountsTableName, result.getTargets().get(0));
        metadataProxy.createTable(customerSpace.toString(), newAccountsTableName, newAccountsTable);
        exportToS3AndAddToContext(newAccountsTable, INTENT_ALERT_NEW_ACCOUNT_TABLE_NAME);

        // Convert all accounts for current week into CSV format
        HdfsDataUnit allAccountsDU = result.getTargets().get(1);
        String destPath = convertToCSV(allAccountsDU);
        // Save all accounts csv path into context
        putStringValueInContext(INTENT_ALERT_ALL_ACCOUNT_TABLE_NAME, destPath);
        log.info("Done with generating intent alert artifacts");
    }

    private String convertToCSV(HdfsDataUnit dataUnit) {
        ConvertToCSVConfig config = new ConvertToCSVConfig();
        config.setCompress(false);
        config.setInput(Collections.singletonList(dataUnit));
        SparkJobResult csvResult = runSparkJob(ConvertToCSVJob.class, config);

        HdfsDataUnit csvDU = csvResult.getTargets().get(0);
        String srcPath = csvDU.getPath();
        String allAccountsTableName = customerSpace.getTenantId() + "_"
                + NamingUtils.timestamp(DataCollectionArtifact.CURRENTWEEK_INTENT_ALLACCOUNTS);
        String destPath = PathBuilder.buildDataTablePath(podId, customerSpace).append(allAccountsTableName).toString();

        try {
            log.info("Moving file from {} to {}", srcPath, destPath);
            HdfsUtils.moveFile(yarnConfiguration, srcPath, destPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to move data from " + srcPath + " to " + destPath);
        }

        return destPath;
    }

    private DataCollectionArtifact getOrCreateArtifact(String artifactsName) {
        DataCollectionArtifact artifact = dataCollectionProxy.getDataCollectionArtifact(customerSpace.toString(),
                artifactsName, activeVersion);
        if (artifact != null) {
            // no need to generate again.
            return artifact;
        } else {
            artifact = new DataCollectionArtifact();
            artifact.setName(artifactsName);
            artifact.setUrl(null);
            artifact.setStatus(GENERATING);
            return dataCollectionProxy.createDataCollectionArtifact(customerSpace.toString(), activeVersion, artifact);
        }
    }
}
