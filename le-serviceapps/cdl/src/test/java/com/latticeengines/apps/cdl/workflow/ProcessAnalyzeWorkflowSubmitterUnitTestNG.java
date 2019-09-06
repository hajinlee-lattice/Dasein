package com.latticeengines.apps.cdl.workflow;

import static com.latticeengines.domain.exposed.pls.ActionType.ACTIVITY_METRICS_CHANGE;
import static com.latticeengines.domain.exposed.pls.ActionType.CDL_OPERATION_WORKFLOW;
import static com.latticeengines.domain.exposed.pls.ActionType.DATA_CLOUD_CHANGE;
import static com.latticeengines.domain.exposed.pls.ActionType.INTENT_CHANGE;
import static com.latticeengines.domain.exposed.pls.ActionType.RATING_ENGINE_CHANGE;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.collections.Sets.newHashSet;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.cdl.entitymgr.CatalogEntityMgr;
import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.cdl.activity.CatalogImport;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.security.Tenant;

public class ProcessAnalyzeWorkflowSubmitterUnitTestNG {

    @Test(groups = "unit", dataProvider = "systemIdMaps")
    private void testGetSystemIdMaps(List<S3ImportSystem> systems, String defaultAcctSysId, String defaultContSysId,
                                     List<String> acctSysIds, List<String> contSysIds) {
        // prepare mocked submitter
        S3ImportSystemService s3ImportSystemService = Mockito.mock(S3ImportSystemService.class);
        when(s3ImportSystemService.getAllS3ImportSystem(any(String.class))).thenReturn(systems);
        ProcessAnalyzeWorkflowSubmitter submitter = mockSubmitter(s3ImportSystemService, null);

        Pair<Map<String, String>, Map<String, List<String>>> res = submitter.getSystemIdMaps("test_tenant", true);
        Assert.assertNotNull(res);
        Assert.assertNotNull(res.getLeft());
        Assert.assertNotNull(res.getRight());

        Map<String, String> defaultIdMap = res.getLeft();
        Map<String, List<String>> systemIdsMap = res.getRight();
        Assert.assertEquals(defaultIdMap.size(), 2, "Default system id map should have size 2 (account + contact)");
        Assert.assertEquals(systemIdsMap.size(), 2, "System ids map should have size 2 (account + contact)");

        String resDefaultAcctId = defaultIdMap.get(Account.name());
        String resDefaultContSysId = defaultIdMap.get(Contact.name());
        // check default system IDs
        Assert.assertEquals(resDefaultAcctId, defaultAcctSysId);
        Assert.assertEquals(resDefaultContSysId, defaultContSysId);

        // check all system ID list
        List<String> resAcctIds = systemIdsMap.get(Account.name());
        List<String> resContIds = systemIdsMap.get(Contact.name());
        Assert.assertEquals(resAcctIds, acctSysIds);
        Assert.assertEquals(resContIds, contSysIds);
    }

    @Test(groups = "unit", dataProvider = "getCatalogImports")
    private void testGetCatalogImports(List<Catalog> catalogs, List<Action> actions,
            Map<String, Set<String>> expectedResult) {
        CatalogEntityMgr mgr = Mockito.mock(CatalogEntityMgr.class);
        when(mgr.findByTenant(any())).thenReturn(catalogs);

        ProcessAnalyzeWorkflowSubmitter submitter = mockSubmitter(null, mgr);
        Map<String, List<CatalogImport>> result = submitter.getCatalogImports(new Tenant(getClass().getSimpleName()),
                actions);
        Assert.assertNotNull(result);

        Map<String, Set<String>> tableNames = result.entrySet().stream().map(entry -> {
            String catalogName = entry.getKey();
            Set<String> tables = entry.getValue().stream().map(CatalogImport::getTableName).collect(Collectors.toSet());
            return Pair.of(catalogName, tables);
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        Assert.assertEquals(tableNames, expectedResult,
                String.format(
                        "Catalog table names for input (catalogs=%s, actions=%s) does not match the expected result",
                        catalogs, actions));
    }

    /**
     * @return [ Input catalogs, Input actions, Expected catalog tablenames ]
     */
    @DataProvider(name = "getCatalogImports")
    private Object[][] getCatalogImportsTestData() {
        return new Object[][] { //
                /*-
                 * empty/null catalogs/actions
                 */
                { null, emptyList(), emptyMap() }, //
                { emptyList(), emptyList(), emptyMap() }, //
                /*-
                 * one import per catalog
                 */
                { //
                        asList(catalog("c1", "t1"), catalog("c2", "t2"), catalog("c3", "t3")), //
                        asList(action(ACTIVITY_METRICS_CHANGE), importAction("t1", asList("table1", "table2")),
                                importAction("t2", singletonList("table3"))), //
                        ImmutableMap.<String, Set<String>> builder().put("c1", newHashSet(asList("table1", "table2")))
                                .put("c2", newHashSet(singletonList("table3"))).build(), //
                }, //
                /*-
                 * multiple imports per catalog
                 */
                { //
                        asList(catalog("c1", "t1"), catalog("c2", "t2"), catalog("c3", "t3")), //
                        asList(importAction("t1", asList("table1", "table2")),
                                importAction("t1", asList("table3", "table4"))), //
                        ImmutableMap.<String, Set<String>> builder()
                                .put("c1", newHashSet(asList("table1", "table2", "table3", "table4"))).build(), //
                }, //
                { //
                        asList(catalog("c1", "t1"), catalog("c2", "t2"), catalog("c3", "t3")), //
                        asList(importAction("t1", asList("table1", "table2")),
                                importAction("t1", asList("table3", "table4")), action(CDL_OPERATION_WORKFLOW),
                                importAction("t3", asList("table5", "table6", "table7")), action(RATING_ENGINE_CHANGE)), //
                        ImmutableMap.<String, Set<String>> builder()
                                .put("c1", newHashSet(asList("table1", "table2", "table3", "table4")))
                                .put("c3", newHashSet(asList("table5", "table6", "table7"))).build(), //
                }, //
                /*-
                 * one import does not have corresponding catalog
                 */
                { //
                        asList(catalog("c1", "t1"), catalog("c3", "t3")), //
                        asList(action(ACTIVITY_METRICS_CHANGE), importAction("t1", asList("table1", "table2")),
                                importAction("t2", singletonList("table3"))), //
                        ImmutableMap.<String, Set<String>> builder().put("c1", newHashSet(asList("table1", "table2")))
                                .build(), //
                }, //
                /*-
                 * all imports are either (a) not import or (b) does not have catalog
                 */
                { //
                        emptyList(), //
                        asList(importAction("t1", asList("table1", "table2")),
                                importAction("t1", asList("table3", "table4")), action(CDL_OPERATION_WORKFLOW),
                                importAction("t3", asList("table5", "table6", "table7")), action(RATING_ENGINE_CHANGE)), //
                        emptyMap(), //
                }, //
                { //
                        asList(catalog("c1", "t1"), catalog("c2", "t2"), catalog("c3", "t3")), //
                        asList(action(CDL_OPERATION_WORKFLOW), importAction("t100", asList("table1", "table2")),
                                action(RATING_ENGINE_CHANGE), action(DATA_CLOUD_CHANGE), action(INTENT_CHANGE)), //
                        emptyMap(), //
                },//
        };
    }

    /**
     * @return Input S3ImportSystem list, Expected default AcctSysId, Expected
     *         default ContSysId, Expected list of account IDs, Expected list of
     *         contact IDs
     */
    @DataProvider(name = "systemIdMaps")
    private Object[][] getSystemIdMapsTestData() {
        return new Object[][] {
                // No S3ImportSystems setup
                { null, null, null, emptyList(), emptyList() }, //
                // No system ID mapped to LatticeAccount or LatticeContact
                { asList( //
                        fakeS3ImportSystem("AID1", false, null, null, 0, null, null), //
                        fakeS3ImportSystem(null, null, "CID2", false, 1, null, null), //
                        fakeS3ImportSystem("AID3", false, "CID3", false, 2, null, null), //
                        null // Theoretical not expect to get null
                        // S3ImportSystem input
                ), null, null, asList("AID1", "AID3"), asList("CID2", "CID3") }, //
                // Has system mapping to LatticeAccount, no mapping to
                // LatticeContact
                { singletonList( //
                        fakeS3ImportSystem("AID1", true, null, null, 0, null, null) //
                ), "AID1", null, singletonList("AID1"), emptyList() }, //
                { singletonList( //
                        fakeS3ImportSystem("AID1", true, "CID1", false, 0, null, null) //
                ), "AID1", null, singletonList("AID1"), singletonList("CID1") }, //
                { asList( //
                        fakeS3ImportSystem("AID1", false, null, null, 0, null, null), //
                        fakeS3ImportSystem("AID2", true, "CID2", false, 0, null, null) //
                ), "AID2", null, asList("AID1", "AID2"), singletonList("CID2") }, //
                // Has system mapping to LatticeContact, no mapping to
                // LatticeAccount
                { singletonList( //
                        fakeS3ImportSystem(null, null, "CID1", true, 0, null, null) //
                ), null, "CID1", emptyList(), singletonList("CID1") }, //
                { singletonList( //
                        fakeS3ImportSystem("AID1", false, "CID1", true, 0, null, null) //
                ), null, "CID1", singletonList("AID1"), singletonList("CID1") }, //
                { asList( //
                        fakeS3ImportSystem(null, null, "CID1", false, 0, null, null), //
                        fakeS3ImportSystem("AID2", false, "CID2", true, 1, null, null) //
                ), null, "CID2", singletonList("AID2"), asList("CID1", "CID2") }, //
                // Has system mapping to LatticeAccount and LatticeContact
                // (might not be same system)
                { asList( //
                        fakeS3ImportSystem("AID1", true, null, null, 0, null, null), //
                        fakeS3ImportSystem("AID2", false, "CID2", true, 1, null, null) //
                ), "AID1", "CID2", asList("AID1", "AID2"), singletonList("CID2") }, //
                { asList( //
                        fakeS3ImportSystem("AID1", false, "CID1", true, 0, null, null), //
                        fakeS3ImportSystem("AID2", true, null, null, 1, null, null) //
                ), "AID2", "CID1", asList("AID1", "AID2"), singletonList("CID1") }, //
                { asList( //
                        fakeS3ImportSystem(null, null, "CID1", false, 0, null, null), //
                        null, // Theoretical not expect to get null
                        // S3ImportSystem input
                        fakeS3ImportSystem("AID2", true, "CID2", true, 1, null, null) //
                ), "AID2", "CID2", singletonList("AID2"), asList("CID1", "CID2") },
                { asList( //
                        fakeS3ImportSystem("AID1", false, null, null, 0, "SAID1", null), //
                        fakeS3ImportSystem(null, null, "CID2", false, 1, null, "SCID1"), //
                        fakeS3ImportSystem("AID3", false, "CID3", false, 2, "SAID2", "SCID2"), //
                        null // Theoretical not expect to get null
                        // S3ImportSystem input
                ), null, null, asList("AID1", "SAID1", "AID3", "SAID2"), asList("CID2", "SCID1", "CID3", "SCID2") },//

        };
    }

    private ProcessAnalyzeWorkflowSubmitter mockSubmitter(S3ImportSystemService s3ImportSystemService,
            CatalogEntityMgr catalogEntityMgr) {
        return new ProcessAnalyzeWorkflowSubmitter(null, null, null, catalogEntityMgr, null, null, null, null, null,
                s3ImportSystemService);
    }

    private Catalog catalog(String catalogName, String dataFeedTaskUniqueId) {
        Catalog catalog = new Catalog();
        catalog.setName(catalogName);
        DataFeedTask task = new DataFeedTask();
        task.setUniqueId(dataFeedTaskUniqueId);
        catalog.setDataFeedTask(task);
        return catalog;
    }

    private Action importAction(String dataFeedTaskUniqueId, List<String> tableNames) {
        Action action = new Action();
        action.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        ImportActionConfiguration config = new ImportActionConfiguration();
        config.setDataFeedTaskId(dataFeedTaskUniqueId);
        config.setRegisteredTables(tableNames);
        action.setActionConfiguration(config);
        return action;
    }

    private Action action(ActionType type) {
        Action action = new Action();
        action.setType(type);
        return action;
    }

    private S3ImportSystem fakeS3ImportSystem(String acctSysId, Boolean mapToLAcct, String contSysId,
                                              Boolean mapToLCont, int priority, String secondaryAcctSysId, String secondaryContSysId) {
        S3ImportSystem sys = new S3ImportSystem();
        sys.setAccountSystemId(acctSysId);
        sys.setMapToLatticeAccount(mapToLAcct);
        sys.setContactSystemId(contSysId);
        sys.setMapToLatticeContact(mapToLCont);
        sys.setPriority(priority);
        if (StringUtils.isNotEmpty(secondaryAcctSysId)) {
            sys.addSecondaryAccountId(EntityType.Accounts, secondaryAcctSysId);
        }
        if (StringUtils.isNotEmpty(secondaryContSysId)) {
            sys.addSecondaryContactId(EntityType.Leads, secondaryContSysId);
        }
        return sys;
    }
}
