package com.latticeengines.apps.cdl.end2end;

import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Enrichment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Segment;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode.Activation;
import static com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode.Usage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;

public class UpdateAccountDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    public static final String CHECK_POINT = "update1";

    // flags for manual testing some cases related to attr activation/deactivation
    private boolean testDeactivateAttrs = false;
    private boolean testActivateAttrs = false;

    private Date segment3Updated;

    @Inject
    private CDLAttrConfigProxy cdlAttrConfigProxy;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint(resumeFromCheckPoint());
        Assert.assertEquals(Long.valueOf(countInRedshift(Account)), getPrePAAccountCount());

        if (testDeactivateAttrs) {
            deactivateAttribute(Account, Arrays.asList("Current_Ratio", "Total_Shareholders_Equity_EUR"));
        }
        if (testActivateAttrs) {
            activateAttribute(Account, //
                    Arrays.asList("FeatureTermBasket", "FeatureTermCart", "FeatureTermProduct"), //
                    Arrays.asList(Segment, Enrichment));
        }

        new Thread(() -> {
            createTestSegment3();
            MetadataSegment segment3 = getSegmentByName(SEGMENT_NAME_3);
            segment3Updated = segment3.getUpdated();
        }).start();

        createSystems();
        importData();
        if (isLocalEnvironment()) {
            processAnalyzeSkipPublishToS3();
        } else {
            processAnalyze();
        }

        if (!(testDeactivateAttrs || testActivateAttrs)) {
            try {
                verifyProcess();
            } finally {
                if (isLocalEnvironment() || persistCheckpoint) {
                    saveCheckpoint(saveToCheckPoint());
                }
            }
        }
    }

    /**
     * Create all {@link S3ImportSystem} required by e2e test
     */
    protected void createSystems() {
        // do nothing
    }

    protected Long getPrePAAccountCount() {
        return ACCOUNT_PA;
    }

    protected void importData() throws Exception {
        mockCSVImport(Account, 2, "DefaultSystem_AccountData");
        Thread.sleep(2000);
        mockCSVImport(Account, 3, "DefaultSystem_AccountData");
        Thread.sleep(2000);
    }

    protected void verifyProcess() {
        clearCache();
        runCommonPAVerifications();
        verifyProcessAnalyzeReport(processAnalyzeAppId, getExpectedReport());
        verifyStats(getEntitiesInStats());
        verifyBatchStore(getExpectedBatchStoreCounts());
        verifyRedshift(getExpectedRedshiftCounts());
        verifyServingStore(getExpectedServingStoreCounts());
        verifyExtraTableRoles(getExtraTableRoeCounts());

        verifySegmentCountsNonNegative(SEGMENT_NAME_3, Arrays.asList(Account, BusinessEntity.Contact));
        MetadataSegment segment3 = getSegmentByName(SEGMENT_NAME_3);
        Assert.assertEquals(segment3Updated, segment3.getUpdated());
        Map<BusinessEntity, Long> segment3Counts = getSegmentCounts(SEGMENT_NAME_3);
        verifyTestSegment3Counts(segment3Counts);
    }

    protected Map<BusinessEntity, Long> getSegmentCounts(String segmentName) {
        if (SEGMENT_NAME_3.equals(segmentName)) {
            return ImmutableMap.of( //
                    Account, SEGMENT_3_ACCOUNT_1, //
                    BusinessEntity.Contact, SEGMENT_3_CONTACT_1);
        }
        throw new IllegalArgumentException(String.format("Segment %s is not supported", segmentName));
    }

    protected BusinessEntity[] getEntitiesInStats() {
        return new BusinessEntity[] { Account, BusinessEntity.Contact, //
                BusinessEntity.PurchaseHistory, BusinessEntity.CuratedAccount, BusinessEntity.CuratedContact };
    }

    protected Map<BusinessEntity, Map<String, Object>> getExpectedReport() {
        Map<String, Object> accountReport = new HashMap<>();
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, NEW_ACCOUNT_UA);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, UPDATED_ACCOUNT_UA);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UNMATCH, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        accountReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, ACCOUNT_UA);

        Map<String, Object> purchaseHistoryReport = new HashMap<>();
        purchaseHistoryReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL,
                TOTAL_PURCHASE_HISTORY_PT);

        Map<String, Object> contactReport = new HashMap<>();
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, 0L);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, 0L);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        contactReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, CONTACT_PA);

        Map<String, Object> productReport = new HashMap<>();
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_ID, 0L);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_HIERARCHY, 0L);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_BUNDLE, 0L);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.ERROR_MESSAGE, "");
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.WARN_MESSAGE, "");

        Map<String, Object> transactionReport = new HashMap<>();
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, 0L);
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        transactionReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL,
                NEW_TRANSACTION_PT);

        Map<BusinessEntity, Map<String, Object>> expectedReport = new HashMap<>();
        expectedReport.put(Account, accountReport);
        expectedReport.put(BusinessEntity.Contact, contactReport);
        expectedReport.put(BusinessEntity.Product, productReport);
        expectedReport.put(BusinessEntity.Transaction, transactionReport);
        expectedReport.put(BusinessEntity.PurchaseHistory, purchaseHistoryReport);

        return expectedReport;
    }

    protected Map<BusinessEntity, Long> getExpectedBatchStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(Account, ACCOUNT_UA);
        map.put(BusinessEntity.Contact, CONTACT_PA);
        map.put(BusinessEntity.Product, BATCH_STORE_PRODUCT_PT);
        map.put(BusinessEntity.Transaction, DAILY_TXN_PT);
        map.put(BusinessEntity.PeriodTransaction, PERIOD_TRANSACTION_PT);
        return map;
    }

    protected Map<BusinessEntity, Long> getExpectedServingStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(Account, ACCOUNT_UA);
        map.put(BusinessEntity.Contact, CONTACT_PA);
        map.put(BusinessEntity.Product, SERVING_STORE_PRODUCTS_PT);
        map.put(BusinessEntity.ProductHierarchy, SERVING_STORE_PRODUCT_HIERARCHIES_PT);
        map.put(BusinessEntity.Transaction, DAILY_TXN_PT);
        map.put(BusinessEntity.PeriodTransaction, PERIOD_TRANSACTION_PT);
        return map;
    }

    protected Map<TableRoleInCollection, Long> getExtraTableRoeCounts() {
        return ImmutableMap.of(//
                TableRoleInCollection.AccountFeatures, ACCOUNT_UA, //
                TableRoleInCollection.AccountExport, ACCOUNT_UA //
        );
    }

    protected Map<BusinessEntity, Long> getExpectedRedshiftCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(Account, ACCOUNT_UA);
        map.put(BusinessEntity.Contact, CONTACT_PA);
        return map;
    }

    protected String resumeFromCheckPoint() {
        return ProcessTransactionDeploymentTestNG.CHECK_POINT;
    }

    protected String saveToCheckPoint() {
        return CHECK_POINT;
    }

    /**
     * Activate the attributes and then enable the flags.
     * Flags argument is optional
     */
    private void activateAttribute(BusinessEntity entity, Collection<String> attrNames, //
                                   Collection<ColumnSelection.Predefined> enabledFlags) {
        AttrConfigRequest request = new AttrConfigRequest();
        List<AttrConfig> attrConfigs = new ArrayList<>();
        for (String attrName: attrNames) {
            AttrConfig attrConfig = new AttrConfig();
            attrConfig.setEntity(entity);
            attrConfig.setAttrName(attrName);
            AttrConfigProp<AttrState> state = new AttrConfigProp<>();
            state.setCustomValue(AttrState.Active);
            attrConfig.putProperty(ColumnMetadataKey.State, state);
            attrConfigs.add(attrConfig);
        }
        request.setAttrConfigs(attrConfigs);
        cdlAttrConfigProxy.saveAttrConfig(mainCustomerSpace, request, Activation);

        request = new AttrConfigRequest();
        attrConfigs = new ArrayList<>();
        for (String attrName: attrNames) {
            AttrConfig attrConfig = new AttrConfig();
            attrConfig.setEntity(entity);
            attrConfig.setAttrName(attrName);
            for (ColumnSelection.Predefined usage : enabledFlags) {
                AttrConfigProp<Boolean> usageState = new AttrConfigProp<>();
                usageState.setCustomValue(Boolean.TRUE);
                attrConfig.putProperty(usage.name(), usageState);
            }
            attrConfigs.add(attrConfig);
        }
        request.setAttrConfigs(attrConfigs);
        cdlAttrConfigProxy.saveAttrConfig(mainCustomerSpace, request, Usage);
    }

    /**
     * Deactivate the attributes.
     * It will disable all usage for the attributes first.
     */
    private void deactivateAttribute(BusinessEntity entity, Collection<String> attrNames) {
        AttrConfigRequest response = cdlAttrConfigProxy.getAttrConfigByEntity(mainCustomerSpace, entity, true);
        List<AttrConfig> attrConfigs = response.getAttrConfigs().stream() //
                .filter(attrConfig -> attrNames.contains(attrConfig.getAttrName())) //
                .collect(Collectors.toList());

        List<AttrConfig> changedAttrs = new ArrayList<>();
        for (AttrConfig attrConfig: attrConfigs) {
            Map<String, AttrConfigProp<?>> changeProps = new HashMap<>();
            for (ColumnSelection.Predefined usage: ColumnSelection.Predefined.values()) {
                boolean enabled = Boolean.TRUE.equals(attrConfig.getPropertyFinalValue(usage.name(), Boolean.class));
                if (enabled) {
                    AttrConfigProp<Boolean> usageState = new AttrConfigProp<>();
                    usageState.setCustomValue(Boolean.FALSE);
                    changeProps.put(usage.name(), usageState);
                }
            }
            if (!changeProps.isEmpty()) {
                attrConfig.setAttrProps(changeProps);
                changedAttrs.add(attrConfig);
            }
        }
        if (!changedAttrs.isEmpty()) {
            AttrConfigRequest request = new AttrConfigRequest();
            cdlAttrConfigProxy.saveAttrConfig(mainCustomerSpace, request, Usage);
        }

        AttrConfigRequest request = new AttrConfigRequest();
        attrConfigs = new ArrayList<>();
        for (String attrName: attrNames) {
            AttrConfig attrConfig = new AttrConfig();
            attrConfig.setEntity(entity);
            attrConfig.setAttrName(attrName);
            AttrConfigProp<AttrState> state = new AttrConfigProp<>();
            state.setCustomValue(AttrState.Inactive);
            attrConfig.putProperty(ColumnMetadataKey.State, state);
            attrConfigs.add(attrConfig);
        }
        request.setAttrConfigs(attrConfigs);
        cdlAttrConfigProxy.saveAttrConfig(mainCustomerSpace, request, Activation);
    }

}
