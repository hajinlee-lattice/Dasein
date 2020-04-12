package com.latticeengines.apps.cdl.end2end;

import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Transaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.DeleteRequest;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.DeleteActionConfiguration;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

/**
 * Going to reuse this test for Soft delete, so the setup step looks redundant
 * for now.
 */
/*
 * dpltc deploy -a pls,admin,cdl,modeling,lp,metadata,workflowapi,eai
 */
public class RegisterDeleteDataDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RegisterDeleteDataDeploymentTestNG.class);

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    private String customerSpace;

    private Set<String> accountIdsForAll = new HashSet<>();
    private Set<String> accountIdsForAccounts = new HashSet<>();
    private Set<String> accountIdsForContacts = new HashSet<>();
    private Set<String> accountIdsForTrxn = new HashSet<>();
    private Set<String> accountIdsForAccountContact = new HashSet<>();
    private Set<String> accountIdsForAccountTrxn = new HashSet<>();
    private Set<String> accountIdsForContactTrxn = new HashSet<>();

    private Set<String> contactIdsForAll = new HashSet<>();
    private Set<String> contactIdsForContacts = new HashSet<>();

    private List<Integer> expectedUploadSize = new ArrayList<>();

    @BeforeClass(groups = "end2end")
    @Override
    public void setup() throws Exception {
        log.info("Running setup with ENABLE_ENTITY_MATCH enabled!");
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), true);
        setupEnd2EndTestEnvironment(featureFlagMap);
        resumeCheckpoint(ProcessTransactionWithAdvancedMatchDeploymentTestNG.CHECK_POINT);
        log.info("Setup Complete!");
        customerSpace = CustomerSpace.parse(mainCustomerSpace).getTenantId();
    }

    @Test(groups = "end2end")
    public void testRegisterDeleteData() {
        extractIds();
        registerDeleteActions();
        verifyRegister();
        processAnalyze();
        verifyAfterPA();
    }

    private void extractIds() {
        String accountTableName = dataCollectionProxy.getTableName(customerSpace,
                TableRoleInCollection.ConsolidatedAccount);
        Table accountTable = metadataProxy.getTableSummary(customerSpace, accountTableName);
        String contactTableName = dataCollectionProxy.getTableName(customerSpace,
                TableRoleInCollection.ConsolidatedContact);
        Table contactTable = metadataProxy.getTableSummary(customerSpace, contactTableName);
        String trxnTableName = dataCollectionProxy.getTableName(customerSpace,
                TableRoleInCollection.ConsolidatedRawTransaction);
        Table trxnTable = metadataProxy.getTableSummary(customerSpace, trxnTableName);

        Iterable<GenericRecord> accountItr = iterateRecords(accountTable);
        Iterable<GenericRecord> contactItr = iterateRecords(contactTable);
        Iterable<GenericRecord> trxnItr = iterateRecords(trxnTable);
        for (GenericRecord record : trxnItr) {
            String id = record.get(InterfaceName.AccountId.name()).toString();
            accountIdsForContactTrxn.add(id);
            if (accountIdsForContactTrxn.size() >= 20) {
                break;
            }
        }
        Set<String> seenIdsInTrxn = new HashSet<>(accountIdsForContactTrxn);
        Set<String> seenIdsInContact = new HashSet<>(accountIdsForContactTrxn);
        seenIdsInTrxn.addAll(accountIdsForTrxn);
        for (GenericRecord record : trxnItr) {
            String id = record.get(InterfaceName.AccountId.name()).toString();
            if (!seenIdsInTrxn.contains(id)) {
                accountIdsForAccountTrxn.add(id);
                if (accountIdsForAccountTrxn.size() >= 20) {
                    break;
                }
            }
        }
        seenIdsInTrxn.addAll(accountIdsForAccountTrxn);
        Set<String> seenIdsInAccount = new HashSet<>(accountIdsForAccountTrxn);
        for (GenericRecord record : trxnItr) {
            String id = record.get(InterfaceName.AccountId.name()).toString();
            if (!seenIdsInTrxn.contains(id)) {
                accountIdsForTrxn.add(id);
                if (accountIdsForTrxn.size() >= 20) {
                    break;
                }
            }
        }

        for (GenericRecord record : contactItr) {
            String accountId = record.get(InterfaceName.AccountId.name()).toString();
            if (!seenIdsInContact.contains(accountId)) {
                accountIdsForContacts.add(accountId);
                if (accountIdsForContacts.size() >= 10) {
                    break;
                }
            }
        }
        seenIdsInContact.addAll(accountIdsForContacts);

        for (GenericRecord record : contactItr) {
            String accountId = record.get(InterfaceName.AccountId.name()).toString();
            String contactId = record.get(InterfaceName.ContactId.name()).toString();
            if (!seenIdsInContact.contains(accountId)) {
                contactIdsForAll.add(contactId);
                if (contactIdsForAll.size() >= 10) {
                    break;
                }
            }
        }
        for (GenericRecord record : contactItr) {
            String accountId = record.get(InterfaceName.AccountId.name()).toString();
            String contactId = record.get(InterfaceName.ContactId.name()).toString();
            if (!seenIdsInContact.contains(accountId)) {
                contactIdsForContacts.add(contactId);
                if (contactIdsForContacts.size() >= 50) {
                    break;
                }
            }
        }

        for (GenericRecord record : accountItr) {
            String id = record.get(InterfaceName.AccountId.name()).toString();
            if (!seenIdsInTrxn.contains(id) && !seenIdsInContact.contains(id) && !seenIdsInAccount.contains(id)) {
                accountIdsForAll.add(id);
                if (accountIdsForAll.size() >= 20) {
                    break;
                }
            }
        }
        seenIdsInAccount.addAll(accountIdsForAll);
        for (GenericRecord record : accountItr) {
            String id = record.get(InterfaceName.AccountId.name()).toString();
            if (!seenIdsInAccount.contains(id)) {
                accountIdsForAccounts.add(id);
                if (accountIdsForAccounts.size() >= 20) {
                    break;
                }
            }
        }
        seenIdsInAccount.addAll(accountIdsForAccounts);
        for (GenericRecord record : accountItr) {
            String id = record.get(InterfaceName.AccountId.name()).toString();
            if (!seenIdsInAccount.contains(id) && !seenIdsInContact.contains(id)) {
                accountIdsForAccountContact.add(id);
                if (accountIdsForAccountContact.size() >= 10) {
                    break;
                }
            }
        }
        seenIdsInAccount.addAll(accountIdsForAccountContact);
        seenIdsInContact.addAll(accountIdsForAccountContact);
    }

    private void registerDeleteActions() {
        registerDeleteAction("account_all.csv", accountIdsForAll, Account, null);
        registerDeleteAction("account_account.csv", accountIdsForAccounts, Account, Collections.singletonList(Account));
        registerDeleteAction("account_contact.csv", accountIdsForContacts, Account, Collections.singletonList(Contact));
        registerDeleteAction("account_trxn.csv", accountIdsForTrxn, Account, Collections.singletonList(Transaction));
        registerDeleteAction("account_account_contact.csv", accountIdsForAccountContact, Account,
                Arrays.asList(Account, Contact));
        registerDeleteAction("account_account_trxn.csv", accountIdsForAccountTrxn, Account,
                Arrays.asList(Account, Transaction));
        registerDeleteAction("account_contact_trx.csv", accountIdsForContactTrxn, Account,
                Arrays.asList(Contact, Transaction));

        registerDeleteAction("contact_all.csv", contactIdsForAll, Contact, null);
        registerDeleteAction("contact_contact.csv", contactIdsForContacts, Contact, Collections.singletonList(Contact));
    }

    private void registerDeleteAction(String fileName, Collection<String> ids, BusinessEntity idEntity,
            List<BusinessEntity> deleteEntities) {
        if (CollectionUtils.isEmpty(ids)) {
            log.info("Only 0 rows in {}. Skip registering the delete action.", fileName);
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("id");
        sb.append('\n');
        for (String id : ids) {
            sb.append(id);
            sb.append('\n');
        }
        log.info("There are " + ids.size() + " rows in " + fileName);
        Resource source = new ByteArrayResource(sb.toString().getBytes()) {
            @Override
            public String getFilename() {
                return fileName;
            }
        };
        SchemaInterpretation template = Account.equals(idEntity) ? SchemaInterpretation.DeleteByAccountTemplate
                : SchemaInterpretation.DeleteByContactTemplate;
        SourceFile sourceFile = uploadDeleteCSV(fileName, template, CleanupOperationType.BYUPLOAD_ID, source);
        DeleteRequest request = new DeleteRequest();
        request.setUser(MultiTenantContext.getEmailAddress());
        request.setFilename(sourceFile.getName());
        request.setIdEntity(idEntity);
        request.setDeleteEntities(deleteEntities);
        request.setHardDelete(false);
        ApplicationId appId = cdlProxy.registerDeleteData(customerSpace, request);
        JobStatus status = waitForWorkflowStatus(appId.toString(), false);
        Assert.assertEquals(JobStatus.COMPLETED, status);
        expectedUploadSize.add(ids.size());
    }

    private void verifyRegister() {
        List<Action> softDeletes = actionProxy.getActions(customerSpace).stream()
                .filter(action -> ActionType.SOFT_DELETE.equals(action.getType())).collect(Collectors.toList());
        Assert.assertNotNull(softDeletes);
        Assert.assertEquals(softDeletes.size(), expectedUploadSize.size());
        log.info("Soft Delet Actions: " + JsonUtils.serialize(softDeletes));

        for (int i = 0; i < expectedUploadSize.size(); i++) {
            DeleteActionConfiguration deleteActionConfiguration = (DeleteActionConfiguration) softDeletes.get(i)
                    .getActionConfiguration();
            Assert.assertNotNull(deleteActionConfiguration);
            String tableName = deleteActionConfiguration.getDeleteDataTable();
            Table registeredDeleteTable = metadataProxy.getTable(customerSpace, tableName);
            Assert.assertNotNull(registeredDeleteTable);
            Integer count = (int) StreamSupport.stream(iterateRecords(registeredDeleteTable).spliterator(), false)
                    .count();
            Assert.assertEquals(count, expectedUploadSize.get(i));
        }
    }

    private void verifyAfterPA() {
        Set<String> accountIdsToDelete = new HashSet<>(accountIdsForAll);
        accountIdsToDelete.addAll(accountIdsForAccounts);
        accountIdsToDelete.addAll(accountIdsForAccountContact);
        accountIdsToDelete.addAll(accountIdsForAccountTrxn);
        Table table = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedAccount);
        for (GenericRecord record : iterateRecords(table)) {
            String accountId = record.get(InterfaceName.AccountId.name()).toString();
            Assert.assertFalse(accountIdsToDelete.contains(accountId), "Should not contain id " + accountId);
        }
        table = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.SystemAccount);
        for (GenericRecord record : iterateRecords(table)) {
            String accountId = record.get(InterfaceName.EntityId.name()).toString();
            Assert.assertFalse(accountIdsToDelete.contains(accountId), "Should not contain id " + accountId);
        }

        accountIdsToDelete = new HashSet<>(accountIdsForAll);
        accountIdsToDelete.addAll(accountIdsForContacts);
        accountIdsToDelete.addAll(accountIdsForAccountContact);
        accountIdsToDelete.addAll(accountIdsForContactTrxn);
        Set<String> contactIdsToDelete = new HashSet<>(contactIdsForAll);
        contactIdsToDelete.addAll(contactIdsForContacts);
        table = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedContact);
        for (GenericRecord record : iterateRecords(table)) {
            String accountId = record.get(InterfaceName.AccountId.name()).toString();
            String contactId = record.get(InterfaceName.ContactId.name()).toString();
            Assert.assertFalse(accountIdsToDelete.contains(accountId), "Should not contain account id " + accountId);
            Assert.assertFalse(contactIdsToDelete.contains(contactId), "Should not contain contact id " + accountId);
        }
        table = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.SystemContact);
        for (GenericRecord record : iterateRecords(table)) {
            String contactId = record.get(InterfaceName.EntityId.name()).toString();
            Assert.assertFalse(accountIdsToDelete.contains(contactId), "Should not contain id " + contactId);
        }

        accountIdsToDelete = new HashSet<>(accountIdsForAll);
        accountIdsToDelete.addAll(accountIdsForTrxn);
        accountIdsToDelete.addAll(accountIdsForAccountTrxn);
        accountIdsToDelete.addAll(accountIdsForContactTrxn);
        table = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedRawTransaction);
        for (GenericRecord record : iterateRecords(table)) {
            String contactId = record.get(InterfaceName.AccountId.name()).toString();
            Assert.assertFalse(accountIdsToDelete.contains(contactId), "Should not contain id " + contactId);
        }
    }

    private Iterable<GenericRecord> iterateRecords(Table table) {
        Assert.assertNotNull(table);
        List<Extract> extracts = table.getExtracts();
        Assert.assertNotNull(extracts);
        List<String> paths = new ArrayList<>();
        for (Extract e : extracts) {
            paths.add(PathUtils.toAvroGlob(e.getPath()));
        }
        return () -> AvroUtils.iterateAvroFiles(yarnConfiguration, paths);
    }

}
