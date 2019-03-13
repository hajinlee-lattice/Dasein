package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_COPIER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PROFILER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CopierConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

@Component(GenerateBucketedAccount.BEAN_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class GenerateBucketedAccount extends BaseSingleEntityProfileStep<ProcessAccountStepConfiguration> {

    static final String BEAN_NAME = "generateBucketedAccount";

    private static final Logger log = LoggerFactory.getLogger(GenerateBucketedAccount.class);

    private int filterStep;
    private int profileStep;
    private int encodeStep;

    private String fullAccountTableName;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Override
    protected TableRoleInCollection profileTableRole() {
        return TableRoleInCollection.Profile;
    }

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        statsTablePrefix = null;
        fullAccountTableName = getStringValueFromContext(FULL_ACCOUNT_TABLE_NAME);
        setEvaluationDateStrAndTimestamp();
    }

    @Override
    protected PipelineTransformationRequest getTransformRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("ProfileAccountStep");
        request.setSubmitter(customerSpace.getTenantId());
        request.setKeepTemp(false);
        request.setEnableSlack(false);
        int step = 0;
        filterStep = step++;
        profileStep = step++;
        encodeStep = step;
        // -----------
        // filter step
        TransformationStepConfig filter = filter();
        TransformationStepConfig profile = profile();
        TransformationStepConfig encode = bucketEncode();

        TransformationStepConfig sortEncode = sortEncode(customerSpace);
        TransformationStepConfig sortProfile = sortProfile(customerSpace, profileTablePrefix);
        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(filter); //
        steps.add(profile); //
        steps.add(encode); //
        steps.add(sortEncode); //
        steps.add(sortProfile); //
        // -----------
        request.setSteps(steps);
        return request;
    }

    private TransformationStepConfig filter() {
        TransformationStepConfig step = new TransformationStepConfig();
        setBaseTables(fullAccountTableName, step);
        step.setTransformer(TRANSFORMER_COPIER);

        List<String> retainAttrNames = servingStoreProxy
                .getDecoratedMetadata(customerSpace.toString(), BusinessEntity.Account, null,
                        tableFromActiveVersion ? active : inactive) //
                .filter(cm -> !AttrState.Inactive.equals(cm.getAttrState())) //
                .filter(cm -> !Boolean.FALSE.equals(cm.getCanSegment())) //
                .map(ColumnMetadata::getAttrName) //
                .collectList().block();
        if (retainAttrNames == null) {
            retainAttrNames = new ArrayList<>();
        }
        if (!retainAttrNames.contains(InterfaceName.LatticeAccountId.name())) {
            retainAttrNames.add(InterfaceName.LatticeAccountId.name());
        }
        if (!retainAttrNames.contains(InterfaceName.AccountId.name())) {
            retainAttrNames.add(InterfaceName.AccountId.name());
        }
        if (!retainAttrNames.contains(InterfaceName.CDLUpdatedTime.name())) {
            retainAttrNames.add(InterfaceName.CDLUpdatedTime.name());
        }

        CopierConfig conf = new CopierConfig();
        conf.setRetainAttrs(retainAttrNames);
        String confStr = appendEngineConf(conf, heavyEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig profile() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(filterStep));
        step.setTransformer(TRANSFORMER_PROFILER);
        ProfileConfig conf = new ProfileConfig();
        conf.setEncAttrPrefix(CEAttr);
        String confStr = appendEngineConf(conf, heavyEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig bucketEncode() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(filterStep, profileStep));
        step.setTransformer(TRANSFORMER_BUCKETER);
        step.setConfiguration(emptyStepConfig(heavyEngineConfig()));
        return step;
    }

    private TransformationStepConfig sortEncode(CustomerSpace customerSpace) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> inputSteps = Collections.singletonList(encodeStep);
        step.setInputSteps(inputSteps);
        step.setTransformer(TRANSFORMER_SORTER);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(servingStoreTablePrefix);
        targetTable.setExpandBucketedAttrs(true);
        step.setTargetTable(targetTable);

        SorterConfig conf = new SorterConfig();
        conf.setPartitions(100);
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
        List<Integer> inputSteps = Collections.singletonList(profileStep);
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
                setupLatticeAccountIdAttr(latticeIdCm, attr);
                dcCount.incrementAndGet();
            } else if (amColMap.containsKey(attr0.getName())) {
                setupAmColMapAttr(amColMap, attr);
                dcCount.incrementAndGet();
            } else if (masterAttrs.containsKey(attr0.getName())) {
                attr = copyMasterAttr(masterAttrs, attr0);
                if (LogicalDataType.Date.equals(attr0.getLogicalDataType())) {
                    log.info("Setting last data refresh for profile date attribute: " + attr.getName() + " to "
                            + evaluationDateStr);
                    attr.setLastDataRefresh("Last Data Refresh: " + evaluationDateStr);
                }
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

    private void setupLatticeAccountIdAttr(ColumnMetadata latticeIdCm, Attribute attr) {
        attr.setInterfaceName(InterfaceName.LatticeAccountId);
        attr.setDisplayName(latticeIdCm.getDisplayName());
        attr.setDescription(latticeIdCm.getDescription());
        attr.setFundamentalType(FundamentalType.NUMERIC);
        attr.setCategory(latticeIdCm.getCategory());
        attr.setGroupsViaList(latticeIdCm.getEnabledGroups());
    }

    private void setupAmColMapAttr(Map<String, ColumnMetadata> amColMap, Attribute attr) {
        ColumnMetadata cm = amColMap.get(attr.getName());
        attr.setDisplayName(removeNonAscII(cm.getDisplayName()));
        attr.setDescription(removeNonAscII(cm.getDescription()));
        attr.setSubcategory(removeNonAscII(cm.getSubcategory()));
        attr.setFundamentalType(cm.getFundamentalType());
        attr.setCategory(cm.getCategory());
        attr.setGroupsViaList(cm.getEnabledGroups());
    }

    private String removeNonAscII(String str) {
        if (StringUtils.isNotBlank(str)) {
            return str.replaceAll("\\P{Print}", "");
        } else {
            return str;
        }
    }

}
