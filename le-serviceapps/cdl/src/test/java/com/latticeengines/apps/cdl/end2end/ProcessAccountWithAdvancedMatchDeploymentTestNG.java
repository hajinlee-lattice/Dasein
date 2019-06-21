package com.latticeengines.apps.cdl.end2end;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.REAL_TIME_MATCH_RECORD_LIMIT;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;

public class ProcessAccountWithAdvancedMatchDeploymentTestNG  extends ProcessAccountDeploymentTestNG {
    private static final Logger log = LoggerFactory.getLogger(ProcessAccountWithAdvancedMatchDeploymentTestNG.class);

    static final String CHECK_POINT = "entitymatch_process1";

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private MatchProxy matchProxy;

    @BeforeClass(groups = "end2end" )
    @Override
    public void setup() throws Exception {
        log.info("Running setup with ENABLE_ENTITY_MATCH enabled!");
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), true);
        setupEnd2EndTestEnvironment(featureFlagMap);
        log.info("Setup Complete!");
    }

    @Override
    protected void importData() throws InterruptedException {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        mockCSVImport(BusinessEntity.Account, ADVANCED_MATCH_SUFFIX, 1, "DefaultSystem_AccountData");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Contact, ADVANCED_MATCH_SUFFIX, 1, "DefaultSystem_ContactData");
        Thread.sleep(2000);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    @Override
    protected String getAvroFileVersion() {
        // advanced matching should use a different version
        return S3_AVRO_VERSION_ADVANCED_MATCH;
    }

    @Override
    protected void verifyProcess() {
        super.verifyProcessAccount();

//        // verify decorated meatadata
//        Set<String> attributes = servingStoreProxy.getDecoratedMetadata(mainCustomerSpace, BusinessEntity.Contact, null)
//                .map(ColumnMetadata::getAttrName).filter(Objects::nonNull).collect(Collectors.toSet()).block();
//        verifyContactAttributes(attributes);
//
//        // verify contact batch/serving table
//        Table contactBatchStoreTable = dataCollectionProxy.getTable(mainCustomerSpace,
//                BusinessEntity.Contact.getBatchStore());
//        Table contactServingStoreTable = dataCollectionProxy.getTable(mainCustomerSpace,
//                BusinessEntity.Contact.getBatchStore());
//        verifyContactTable(contactBatchStoreTable);
//        verifyContactTable(contactServingStoreTable);

//        // make sure account seed/lookup data is published to serving dynamo table
//        verifyAccountSeedLookupData();
    }

    private void verifyAccountSeedLookupData() {
        try {
            List<String> accountIds = getAccountIds(1, REAL_TIME_MATCH_RECORD_LIMIT);
            MatchInput input = getMatchInput();
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
            // make sure all accountIds can match to some account (has non-blank entityId)
            Assert.assertEquals(entityIds.size(), accountIds.size());
            for (String entityId : entityIds) {
                Assert.assertTrue(StringUtils.isNotBlank(entityId));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private MatchInput getMatchInput() {
        MatchInput input = new MatchInput();
        input.setDataCloudVersion(columnMetadataProxy.latestVersion().getVersion());
        input.setTenant(mainTestTenant);
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

    private List<String> getAccountIds(int fileIdx, int recordLimit) throws Exception {
        Pair<String, InputStream> testArtifact = getTestAvroFile(BusinessEntity.Account, ADVANCED_MATCH_SUFFIX, fileIdx);
        List<GenericRecord> records = AvroUtils.readFromInputStream(testArtifact.getRight(), 0, recordLimit);
        return records.stream().map(record -> record.get(InterfaceName.AccountId.name()).toString())
                .collect(Collectors.toList());
    }

    private void verifyContactTable(Table table) {
        Assert.assertNotNull(table);
        Assert.assertNotNull(table.getAttributes());
        Set<String> attrs = table.getAttributes().stream().map(Attribute::getInterfaceName).filter(Objects::nonNull)
                .map(Enum::name).collect(Collectors.toSet());
        verifyContactAttributes(attrs);
    }

    private void verifyContactAttributes(Set<String> attrNames) {
        // should have AccountId & CustomerAccountId attributes but not account match
        // keys
        InterfaceName[] attrsInTable = new InterfaceName[] { InterfaceName.AccountId, InterfaceName.CustomerAccountId };
        InterfaceName[] attrsNotInTable = new InterfaceName[] { InterfaceName.Website, InterfaceName.DUNS,
                InterfaceName.CompanyName, InterfaceName.State, InterfaceName.Country, InterfaceName.City };
        Assert.assertNotNull(attrNames);
        for (InterfaceName attr : attrsInTable) {
            Assert.assertTrue(attrNames.contains(attr.name()));
        }
        for (InterfaceName attr : attrsNotInTable) {
            Assert.assertFalse(attrNames.contains(attr.name()));
        }
    }

    @Override
    protected BusinessEntity[] getEntitiesInStats() {
        return new BusinessEntity[] { BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.CuratedAccount };
    }

    @Override
    protected Map<BusinessEntity, Map<String, Object>> getExpectedReport() {
        Map<String, Object> accountReport = new HashMap<>();
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.NEW,
                ACCOUNT_PA);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.UPDATE, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.UNMATCH, 8L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.DELETE, 0L);
        accountReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + UNDER_SCORE + ReportConstants.TOTAL, ACCOUNT_PA_EM);

        Map<String, Object> contactReport = new HashMap<>();
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, CONTACT_PA_EM);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, 0L);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        contactReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, CONTACT_PA_EM);

        Map<BusinessEntity, Map<String, Object>> expectedReport = new HashMap<>();
        expectedReport.put(BusinessEntity.Account, accountReport);
        expectedReport.put(BusinessEntity.Contact, contactReport);

        return expectedReport;
    }

    @Override
    protected Map<BusinessEntity, Long> getExpectedBatchStoreCounts() {
        return ImmutableMap.of( //
                BusinessEntity.Account, ACCOUNT_PA_EM, //
                BusinessEntity.Contact, CONTACT_PA_EM //
        );
    }

    @Override
    protected Map<BusinessEntity, Long> getExpectedServingStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_PA_EM);
        map.put(BusinessEntity.Contact, CONTACT_PA_EM);
        return map;
    }

    @Override
    protected Map<TableRoleInCollection, Long> getExtraTableRoeCounts() {
        return ImmutableMap.of(//
                TableRoleInCollection.AccountFeatures, ACCOUNT_PA_EM, //
                TableRoleInCollection.AccountExport, ACCOUNT_PA_EM //
        );
    }

    @Override
    protected Map<BusinessEntity, Long> getExpectedRedshiftCounts() {
        return ImmutableMap.of( //
                BusinessEntity.Account, ACCOUNT_PA_EM, //
                BusinessEntity.Contact, CONTACT_PA_EM //
        );
    }

    @Override
    protected String saveToCheckPoint() {
        return CHECK_POINT;
    }
}
