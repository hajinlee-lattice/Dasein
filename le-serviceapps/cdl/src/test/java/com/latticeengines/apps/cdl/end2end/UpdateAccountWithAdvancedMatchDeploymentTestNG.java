package com.latticeengines.apps.cdl.end2end;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.REAL_TIME_MATCH_RECORD_LIMIT;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.cdl.service.impl.CheckpointService;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.AvroUtils.AvroFilesIterator;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;

public class UpdateAccountWithAdvancedMatchDeploymentTestNG extends UpdateAccountDeploymentTestNG {

    private static final Logger log = LoggerFactory.getLogger(UpdateAccountWithAdvancedMatchDeploymentTestNG.class);
    private static final long SEGMENT3_ACCOUNT_CNT = 46;
    private static final long SEGMENT3_CONTACT_CNT = 46;

    static final String CHECK_POINT = "entitymatch_update1";

    @Inject
    private MatchProxy matchProxy;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @BeforeClass(groups = "end2end")
    @Override
    public void setup() throws Exception {
        log.info("Running setup with ENABLE_ENTITY_MATCH enabled!");
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), true);
        setupEnd2EndTestEnvironment(featureFlagMap);
        log.info("Setup Complete!");
    }

    @Override
    protected void createSystems() {
        mockImportSystem("DefaultSystem");
        mockImportSystem("Mkto");
    }

    @Override
    protected Long getPrePAAccountCount() {
        return ACCOUNT_PA_EM;
    }

    @Override
    protected void importData() throws Exception {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        mockCSVImport(BusinessEntity.Account, ADVANCED_MATCH_SUFFIX, 2, "DefaultSystem_AccountData");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Account, ADVANCED_MATCH_SUFFIX, 98, "DefaultSystem_AccountData_Test");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Account, ADVANCED_MATCH_SUFFIX, 99, "Mkto_AccountData_Test");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Contact, ADVANCED_MATCH_SUFFIX, 2, "DefaultSystem_ContactData");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Contact, ADVANCED_MATCH_SUFFIX, 98, "DefaultSystem_ContactData_Test");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Contact, ADVANCED_MATCH_SUFFIX, 99, "Mkto_ContactData_Test");
        Thread.sleep(2000);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    @Override
    protected String getAvroFileVersion() {
        // advanced matching should use a different version
        return S3_AVRO_VERSION_ADVANCED_MATCH;
    }

    @Override
    protected String resumeFromCheckPoint() {
        return ProcessAccountWithAdvancedMatchDeploymentTestNG.CHECK_POINT;
    }

    @Override
    protected String saveToCheckPoint() {
        return CHECK_POINT;
    }

    @Override
    protected void verifyProcess() {
        super.verifyProcess();
//        verifyBatchStoreIds();
//        verifyAccountSeedLookupData();
    }

    // new/update/total #contact in report
    @Override
    protected Map<BusinessEntity, Map<String, Object>> getExpectedReport() {
        String summaryPrefix = ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_";

        Map<String, Object> accountReport = new HashMap<>();
        accountReport.put(summaryPrefix + ReportConstants.NEW, NEW_ACCOUNT_UA_EM);
        accountReport.put(summaryPrefix + ReportConstants.UPDATE, UPDATED_ACCOUNT_UA_EM);
        accountReport.put(summaryPrefix + ReportConstants.UNMATCH, 1L);
        accountReport.put(summaryPrefix + ReportConstants.DELETE, 0L);
        accountReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, ACCOUNT_UA_EM);
        accountReport.put(ReportPurpose.ENTITY_MATCH_SUMMARY.name() + "_" + ReportConstants.PUBLISH_SEED, UPDATED_ACCOUNT_UA_EM);

        Map<String, Object> contactReport = new HashMap<>();
        contactReport.put(summaryPrefix + ReportConstants.NEW, NEW_CONTACT_UA_EM);
        contactReport.put(summaryPrefix + ReportConstants.UPDATE, UPDATED_CONTACT_UA_EM);
        contactReport.put(summaryPrefix + ReportConstants.DELETE, 0L);
        contactReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, CONTACT_UA_EM);

        Map<BusinessEntity, Map<String, Object>> expectedReport = new HashMap<>();
        expectedReport.put(BusinessEntity.Account, accountReport);
        expectedReport.put(BusinessEntity.Contact, contactReport);

        return expectedReport;
    }

    @Override
    protected BusinessEntity[] getEntitiesInStats() {
        return new BusinessEntity[] { BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.CuratedAccount };
    }

    @Override
    protected Map<BusinessEntity, Long> getExpectedBatchStoreCounts() {
        return ImmutableMap.of(//
                BusinessEntity.Account, ACCOUNT_UA_EM, //
                BusinessEntity.Contact, CONTACT_UA_EM);
    }

    @Override
    protected Map<BusinessEntity, Long> getExpectedServingStoreCounts() {
        return ImmutableMap.of(//
                BusinessEntity.Account, ACCOUNT_UA_EM, //
                BusinessEntity.Contact, CONTACT_UA_EM);
    }

    @Override
    protected Map<TableRoleInCollection, Long> getExtraTableRoeCounts() {
        return ImmutableMap.of(//
                TableRoleInCollection.AccountFeatures, ACCOUNT_UA_EM, //
                TableRoleInCollection.AccountExport, ACCOUNT_UA_EM //
        );
    }

    @Override
    protected Map<BusinessEntity, Long> getExpectedRedshiftCounts() {
        return ImmutableMap.of(//
                BusinessEntity.Account, ACCOUNT_UA_EM, //
                BusinessEntity.Contact, CONTACT_UA_EM);
    }

    @Override
    protected Map<BusinessEntity, Long> getSegmentCounts(String segmentName) {
        // only verify segment 3 for now
        if (SEGMENT_NAME_3.equals(segmentName)) {
            return ImmutableMap.of( //
                    BusinessEntity.Account, SEGMENT3_ACCOUNT_CNT, BusinessEntity.Contact, SEGMENT3_CONTACT_CNT);
        }
        throw new IllegalArgumentException(String.format("Segment %s is not supported", segmentName));
    }

    private void verifyBatchStoreIds() {
        Table consolidatedAccount = dataCollectionProxy.getTable(CustomerSpace.parse(mainTestTenant.getId()).toString(),
                TableRoleInCollection.ConsolidatedAccount);
        String hdfsPath = consolidatedAccount.getExtracts().get(0).getPath();
        log.info("Account batch store {} location: {}", consolidatedAccount.getName(), hdfsPath);
        AvroFilesIterator iter = AvroUtils.iterateAvroFiles(yarnConfiguration, hdfsPath);
        // Currently use AccountId as record identifier
        Set<String> recordIdsNoEntityId = new HashSet<>();
        int total = 0;
        int cntNoLDCId = 0;
        while (iter.hasNext()) {
            GenericRecord record = iter.next();
            Assert.assertNotNull(record.get(InterfaceName.AccountId.name()));
            if (record.get(InterfaceName.EntityId.name()) == null) {
                recordIdsNoEntityId.add(record.get(InterfaceName.AccountId.name()).toString());
            }
            if (record.get(InterfaceName.LatticeAccountId.name()) == null) {
                cntNoLDCId++;
            }
            total++;
        }
        log.info("Account batch store {}: {}/{} records don't have EntityId, " //
                + "{}/{} records don't have LatticeAccountId, " //
                + "AccountId of records without EntityId: {}", consolidatedAccount.getName(),
                recordIdsNoEntityId.size(), total, cntNoLDCId, total,
                CollectionUtils.isEmpty(recordIdsNoEntityId) ? "None" : String.join(",", recordIdsNoEntityId));
        Assert.assertEquals(recordIdsNoEntityId.size(), 0);
        Assert.assertTrue((double) cntNoLDCId / total <= 0.05);
    }

    private void verifyAccountSeedLookupData() {
        try {
            // AIDs imported in ProcessAccount PA only:
            //      Testing tenant and checkpoint tenant should match to same EntityId
            // AIDs imported in UpdateAccount PA only:
            //      Testing tenant should all match to non-empty EntityId
            // AIDs imported in both ProcessAccount and UpdateAccount PA:
            //      Testing tenant and checkpoint tenant should match to same EntityId
            Triple<List<String>, List<String>, List<String>> aidLists = getAccountIds(REAL_TIME_MATCH_RECORD_LIMIT);
            String checkpointTenantId = CheckpointService.getCheckPointTenantId(
                    ProcessAccountWithAdvancedMatchDeploymentTestNG.CHECK_POINT,
                    String.valueOf(CDLEnd2EndDeploymentTestNGBase.S3_CHECKPOINTS_VERSION),
                    BusinessEntity.Account.name());
            verifyMatchToSameEntityId(mainTestTenant.getId(), checkpointTenantId, aidLists.getLeft());
            verifyMatchToSameEntityId(mainTestTenant.getId(), checkpointTenantId, aidLists.getRight());
            verifyMatchToEntityId(mainTestTenant.getId(), aidLists.getMiddle());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param recordLimit
     * @return (AIDs imported in ProcessAccount PA only,
     *          AIDs imported in UpdateAccount PA only,
     *          AIDs imported in both ProcessAccount and UpdateAccount PA)
     * @throws Exception
     */
    private Triple<List<String>, List<String>, List<String>> getAccountIds(int recordLimit) throws Exception {
        // Currently there are totally 1000 accounts only. Reading all into
        // memory is fine. If account test artifact is enlarged in future, might
        // change to iterator
        List<GenericRecord> records1stPA = AvroUtils
                .readFromInputStream(getTestAvroFile(BusinessEntity.Account, 1).getRight());
        records1stPA.addAll(AvroUtils.readFromInputStream(getTestAvroFile(BusinessEntity.Account, 2).getRight()));
        List<GenericRecord> records2ndPA = AvroUtils
                .readFromInputStream(getTestAvroFile(BusinessEntity.Account, 3).getRight());
        Set<String> aids1stPA = records1stPA.stream()
                .map(record -> record.get(InterfaceName.AccountId.name()).toString()).collect(Collectors.toSet());
        Set<String> aids2ndPA = records2ndPA.stream()
                .map(record -> record.get(InterfaceName.AccountId.name()).toString()).collect(Collectors.toSet());
        List<String> aids1stPAOnly = aids1stPA.stream().filter(aid -> !aids2ndPA.contains(aid)).limit(recordLimit)
                .collect(Collectors.toList());
        List<String> aids2ndPAOnly = aids2ndPA.stream().filter(aid -> !aids1stPA.contains(aid)).limit(recordLimit)
                .collect(Collectors.toList());
        List<String> aidsBothPA = aids1stPA.stream().filter(aid -> aids2ndPA.contains(aid)).limit(recordLimit)
                .collect(Collectors.toList());
        return Triple.of(aids1stPAOnly, aids2ndPAOnly, aidsBothPA);
    }

    private List<String> verifyMatchToEntityId(String tenantId, List<String> accountIds) {
        MatchInput input = getMatchInput(tenantId);
        // set match data
        input.setData(accountIds.stream() //
                .map(Object.class::cast) //
                .map(Collections::singletonList) //
                .collect(Collectors.toList()));
        MatchOutput output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);
        Assert.assertNotNull(output.getResult());
        List<String> entityIds = output.getResult() //
                .stream() //
                .map(OutputRecord::getOutput) //
                .filter(Objects::nonNull) //
                .map(Object::toString) //
                .collect(Collectors.toList());
        // make sure all accountIds can match to some account (has non-blank
        // entityId)
        Assert.assertEquals(entityIds.size(), accountIds.size());
        entityIds.forEach(entityId -> {
            Assert.assertTrue(StringUtils.isNotBlank(entityId));
        });
        return entityIds;
    }

    private void verifyMatchToSameEntityId(String tenantId1, String tenantId2, List<String> accountIds) {
        List<String> entityIds1 = verifyMatchToEntityId(tenantId1, accountIds);
        List<String> entityIds2 = verifyMatchToEntityId(tenantId2, accountIds);
        Assert.assertTrue(CollectionUtils.isEqualCollection(entityIds1, entityIds2));
    }

    private MatchInput getMatchInput(String tenantId) {
        MatchInput input = new MatchInput();
        input.setDataCloudVersion(columnMetadataProxy.latestVersion().getVersion());
        input.setTenant(new Tenant(tenantId));
        input.setOperationalMode(OperationalMode.ENTITY_MATCH);
        input.setPredefinedSelection(ColumnSelection.Predefined.ID);
        input.setTargetEntity(BusinessEntity.Account.name());
        input.setAllocateId(false);
        input.setSkipKeyResolution(true);
        input.setFetchOnly(false);
        input.setUseRemoteDnB(true);
        input.setUseDnBCache(true);

        // set entity key map
        MatchInput.EntityKeyMap map = new MatchInput.EntityKeyMap();
        map.setKeyMap(
                Collections.singletonMap(MatchKey.SystemId, Collections.singletonList(InterfaceName.AccountId.name())));
        input.setEntityKeyMaps(Collections.singletonMap(BusinessEntity.Account.name(), map));

        // set field
        input.setFields(Collections.singletonList(InterfaceName.AccountId.name()));
        return input;
    }
}
