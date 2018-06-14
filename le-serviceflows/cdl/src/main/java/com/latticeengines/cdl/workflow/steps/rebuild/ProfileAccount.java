package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_COPIER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PROFILER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_STATS_CALCULATOR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CalculateStatsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CopierConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

@Component(ProfileAccount.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfileAccount extends BaseSingleEntityProfileStep<ProcessAccountStepConfiguration> {

    static final String BEAN_NAME = "profileAccount";

    private static final Logger log = LoggerFactory.getLogger(ProfileAccount.class);

    private static int matchStep;
    private static int profileStep;
    private static int bucketStep;
    private static int filterStep;
    private static int segmentProfileStep;
    private static int segmentBucketStep;

    private List<ColumnMetadata> allAttrs;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Override
    protected TableRoleInCollection profileTableRole() {
        return TableRoleInCollection.Profile;
    }

    @Override
    protected void onPostTransformationCompleted() {
        super.onPostTransformationCompleted();
        registerDynamoExport();
    }

    @Override
    protected PipelineTransformationRequest getTransformRequest() {
        String masterTableName = masterTable.getName();
        try {
            allAttrs = servingStoreProxy
                    .getDecoratedMetadata(customerSpace.toString(), BusinessEntity.Account, null, inactive)
                    .filter(cm -> !AttrState.Inactive.equals(cm.getAttrState()))
                    .collectList().block();

            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("ProfileAccountStep");
            request.setSubmitter(customerSpace.getTenantId());
            request.setKeepTemp(false);
            request.setEnableSlack(false);
            matchStep = 0;
            profileStep = 1;
            bucketStep = 2;
            filterStep = 4;
            segmentProfileStep = 5;
            segmentBucketStep = 6;
            // -----------
            TransformationStepConfig match = match(customerSpace, masterTableName);
            TransformationStepConfig profile = profile();
            TransformationStepConfig bucket = bucket();
            TransformationStepConfig calc = calcStats(customerSpace, statsTablePrefix);

            //filter step
            TransformationStepConfig filter = filter(customerSpace);
            TransformationStepConfig segmentProfile = segmentProfile();
            TransformationStepConfig segmentBucket = segmentBucket();

            TransformationStepConfig sort = sort(customerSpace);
            TransformationStepConfig sortProfile = sortProfile(customerSpace, profileTablePrefix);
            // -----------
            List<TransformationStepConfig> steps = Arrays.asList( //
                    match, //
                    profile, //
                    bucket, //
                    calc, //
                    filter,
                    segmentProfile,
                    segmentBucket,
                    sort, //
                    sortProfile //
            );
            // -----------
            request.setSteps(steps);
            return request;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig match(CustomerSpace customerSpace, String sourceTableName) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MATCH);

        String tableSourceName = "CustomerUniverse";
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);

        MatchTransformerConfig config = new MatchTransformerConfig();
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(customerSpace.toString()));


        String dataCloudVersion = "";
        DataCollectionStatus detail = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (detail != null) {
            try {
                dataCloudVersion = DataCloudVersion.parseBuildNumber(detail.getDataCloudBuildNumber()).getVersion();
            } catch (Exception e) {
                log.warn("Failed to read datacloud version from collection status " + JsonUtils.serialize(detail));
            }
        }

        List<ColumnMetadata> dcCols = columnMetadataProxy.getAllColumns(dataCloudVersion);
        Set<String> allAttrNames = allAttrs.stream().map(ColumnMetadata::getAttrName).collect(Collectors.toSet());
        List<Column> cols = new ArrayList<>();
        for (ColumnMetadata cm : dcCols) {
            if (allAttrNames.contains(cm.getAttrName())) {
                cols.add(new Column(cm.getAttrName()));
            }
        }
        ColumnSelection cs = new ColumnSelection();
        cs.setColumns(cols);

        matchInput.setCustomSelection(cs);
        matchInput.setUnionSelection(null);
        matchInput.setPredefinedSelection(null);
        matchInput.setKeyMap(getKeyMap());
        matchInput.setDataCloudVersion(getDataCloudVersion());
        matchInput.setSkipKeyResolution(true);
        matchInput.setFetchOnly(true);
        matchInput.setSplitsPerBlock(cascadingPartitions * 10);
        config.setMatchInput(matchInput);
        step.setConfiguration(JsonUtils.serialize(config));

        return step;
    }

    private TransformationStepConfig profile() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(matchStep));
        step.setTransformer(TRANSFORMER_PROFILER);
        ProfileConfig conf = new ProfileConfig();
        conf.setEncAttrPrefix(CEAttr);
        String confStr = appendEngineConf(conf, heavyEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig bucket() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(matchStep, profileStep));
        step.setTransformer(TRANSFORMER_BUCKETER);
        step.setConfiguration(emptyStepConfig(heavyEngineConfig()));
        return step;
    }

    private TransformationStepConfig segmentProfile() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(filterStep));
        step.setTransformer(TRANSFORMER_PROFILER);
        ProfileConfig conf = new ProfileConfig();
        conf.setEncAttrPrefix(CEAttr);
        String confStr = appendEngineConf(conf, heavyEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig segmentBucket() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(filterStep, segmentProfileStep));
        step.setTransformer(TRANSFORMER_BUCKETER);
        step.setConfiguration(emptyStepConfig(heavyEngineConfig()));
        return step;
    }

    private TransformationStepConfig calcStats(CustomerSpace customerSpace, String statsTablePrefix) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(bucketStep, profileStep));
        step.setTransformer(TRANSFORMER_STATS_CALCULATOR);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(statsTablePrefix);
        step.setTargetTable(targetTable);

        CalculateStatsConfig conf = new CalculateStatsConfig();
        step.setConfiguration(appendEngineConf(conf, heavyEngineConfig()));
        return step;
    }

    private TransformationStepConfig filter(CustomerSpace customerSpace) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> inputSteps = Collections.singletonList(matchStep);
        step.setInputSteps(inputSteps);
        step.setTransformer(TRANSFORMER_COPIER);

        DataCollection.Version inactive = dataCollectionProxy.getInactiveVersion(customerSpace.toString());

        CopierConfig conf = new CopierConfig();
        List<ColumnMetadata> allAttrs = servingStoreProxy.getDecoratedMetadata(customerSpace.toString(),
                BusinessEntity.Account, null, inactive).collectList().block();
        Set<ColumnSelection.Predefined> filterGroups = new HashSet<>();
        filterGroups.add(ColumnSelection.Predefined.ID);
        filterGroups.add(ColumnSelection.Predefined.LookupId);

        List<ColumnMetadata> retainAttrs = allAttrs.stream().filter(cm ->
                (!AttrState.Inactive.equals(cm.getAttrState()) && Boolean.TRUE.equals(cm.getCanSegment()))
                || filterGroups.stream().anyMatch(cm::isEnabledFor)).collect(Collectors.toList());
        List<String> retainAttrNames = retainAttrs.stream().map(segmentAttr -> segmentAttr.getAttrName())
                .collect(Collectors.toList());
        retainAttrNames.add(InterfaceName.AccountId.name());

        conf.setRetainAttrs(retainAttrNames);
        String confStr = appendEngineConf(conf, heavyEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig sort(CustomerSpace customerSpace) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> inputSteps = Collections.singletonList(segmentBucketStep);
        step.setInputSteps(inputSteps);
        step.setTransformer(TRANSFORMER_SORTER);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(servingStoreTablePrefix);
        targetTable.setExpandBucketedAttrs(true);
        step.setTargetTable(targetTable);

        SorterConfig conf = new SorterConfig();
        conf.setPartitions(500);
        conf.setSplittingThreads(maxSplitThreads);
        conf.setSplittingChunkSize(5000L);
        conf.setCompressResult(true);
        conf.setSortingField(servingStoreSortKey);
        String confStr = appendEngineConf(conf, heavyEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig sortProfile(CustomerSpace customerSpace, String profileTablePrefix) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> inputSteps = Collections.singletonList(segmentProfileStep);
        step.setInputSteps(inputSteps);
        step.setTransformer(TRANSFORMER_SORTER);

        SorterConfig conf = new SorterConfig();
        conf.setPartitions(1);
        conf.setCompressResult(true);
        conf.setSortingField(DataCloudConstants.PROFILE_ATTR_ATTRNAME);
        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(profileTablePrefix);
        step.setTargetTable(targetTable);

        return step;
    }

    private Map<MatchKey, List<String>> getKeyMap() {
        Map<MatchKey, List<String>> keyMap = new TreeMap<>();
        keyMap.put(MatchKey.LatticeAccountID, Collections.singletonList(InterfaceName.LatticeAccountId.name()));
        return keyMap;
    }

    @Override
    protected void enrichTableSchema(Table table) {
        String dataCloudVersion = configuration.getDataCloudVersion();
        List<ColumnMetadata> amCols = columnMetadataProxy.columnSelection(ColumnSelection.Predefined.Segment,
                dataCloudVersion);
        Map<String, ColumnMetadata> amColMap = new HashMap<>();
        amCols.forEach(cm -> amColMap.put(cm.getColumnId(), cm));
        ColumnMetadata latticeIdCm = columnMetadataProxy
                .columnSelection(ColumnSelection.Predefined.ID, dataCloudVersion).get(0);
        Map<String, Attribute> masterAttrs = new HashMap<>();
        masterTable.getAttributes().forEach(attr -> {
            masterAttrs.put(attr.getName(), attr);
        });
        List<Attribute> attrs = new ArrayList<>();
        final AtomicLong dcCount = new AtomicLong(0);
        final AtomicLong masterCount = new AtomicLong(0);
        table.getAttributes().forEach(attr0 -> {
            Attribute attr = attr0;
            if (InterfaceName.LatticeAccountId.name().equals(attr0.getName())) {
                attr.setInterfaceName(InterfaceName.LatticeAccountId);
                attr.setDisplayName(latticeIdCm.getDisplayName());
                attr.setDescription(latticeIdCm.getDescription());
                attr.setFundamentalType(FundamentalType.NUMERIC);
                attr.setCategory(latticeIdCm.getCategory());
                attr.setGroupsViaList(latticeIdCm.getEnabledGroups());
                dcCount.incrementAndGet();
            } else if (amColMap.containsKey(attr0.getName())) {
                ColumnMetadata cm = amColMap.get(attr.getName());
                attr.setDisplayName(removeNonAscII(cm.getDisplayName()));
                attr.setDescription(removeNonAscII(cm.getDescription()));
                attr.setSubcategory(removeNonAscII(cm.getSubcategory()));
                attr.setFundamentalType(cm.getFundamentalType());
                attr.setCategory(cm.getCategory());
                attr.setGroupsViaList(cm.getEnabledGroups());
                dcCount.incrementAndGet();
            } else if (masterAttrs.containsKey(attr0.getName())) {
                attr = copyMasterAttr(masterAttrs, attr0);
                masterCount.incrementAndGet();
            }
            if (StringUtils.isBlank(attr.getCategory())) {
                attr.setCategory(Category.ACCOUNT_ATTRIBUTES);
            }
            if (Category.ACCOUNT_ATTRIBUTES.name().equals(attr.getCategory())) {
                attr.setSubcategory(null);
            }
            attr.removeAllowedDisplayNames();
            attrs.add(attr);
        });
        table.setAttributes(attrs);
        log.info("Enriched " + dcCount.get() + " attributes using data cloud metadata.");
        log.info("Copied " + masterCount.get() + " attributes from batch store metadata.");
        log.info("BucketedAccount table has " + table.getAttributes().size() + " attributes in total.");
    }

    private String removeNonAscII(String str) {
        if (StringUtils.isNotBlank(str)) {
            return str.replaceAll("\\P{Print}", "");
        } else {
            return str;
        }
    }

    private void registerDynamoExport() {
        exportToDynamo( masterTable.getName(), InterfaceName.AccountId.name(), null);
    }

}
