package com.latticeengines.datafabric.entitymanager.impl;

import static com.latticeengines.datafabric.entitymanager.impl.TestDynamoEntityMgrImpl.RECORD_TYPE;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.datafabric.entitymanager.CompositeGraphEntityMgr;
import com.latticeengines.datafabric.functionalframework.DataFabricFunctionalTestNGBase;
import com.latticeengines.datafabric.service.datastore.impl.DynamoDataStoreImpl;
import com.latticeengines.domain.exposed.datafabric.CompositeFabricEntity;
import com.latticeengines.domain.exposed.datafabric.CompositeGraphEntity;


public class TestDynamoGraphEntityMgrTestNG extends DataFabricFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(TestDynamoGraphEntityMgrTestNG.class);

    @Inject
    private DynamoService dynamoService;

    @Value("${common.le.environment}")
    private String leEnv;

    @Value("${common.le.stack}")
    private String leStack;

    private String repo;
    private String tableName;

    private AccountEntityMgr accountMgr;
    private ContactEntityMgr contactMgr;
    private HistoryEntityMgr historyMgr;
    private CompositeGraphEntityMgr graphMgr;

    @BeforeClass(groups = "dynamo")
    public void setup() throws Exception {
        repo = leEnv + "_" + leStack + "_testRepo";
        tableName = DynamoDataStoreImpl.buildTableName(repo, RECORD_TYPE);
        dynamoService.deleteTable(tableName);

        dynamoService.createTable(tableName, 10, 10, "parentKey", ScalarAttributeType.S.name(), "entityId",
                ScalarAttributeType.S.name(), null);
        ListTablesResult result = dynamoService.getClient().listTables();
        log.info("Tables: " + result.getTableNames());


        BaseFabricEntityMgrImpl.Builder builder = new BaseFabricEntityMgrImpl.Builder()
                .dataService(dataService) //
                .recordType(RECORD_TYPE) //
                .store("DYNAMO").repository(repo);


        log.info("AccountEntityMgr");
        accountMgr = new AccountEntityMgr(builder);
        accountMgr.init();
        log.info("ContactEntityMgr");
        contactMgr = new ContactEntityMgr(builder);
        contactMgr.init();
        log.info("HistoryEntityMgr");
        historyMgr = new HistoryEntityMgr(builder);
        historyMgr.init();

        CompositeGraphEntityMgr historyGraphMgr = new CompositeGraphEntityMgrImpl("history", historyMgr);
        CompositeGraphEntityMgr contactGraphMgr = new CompositeGraphEntityMgrImpl("contact", contactMgr);
        contactGraphMgr.addChild(historyGraphMgr);
        graphMgr = new CompositeGraphEntityMgrImpl("account", accountMgr);
        graphMgr.addChild(contactGraphMgr);
    }

    @AfterClass(groups = "dynamo")
    public void teardown() throws Exception {
        dynamoService.deleteTable(tableName);
    }

    @Test(groups = "dynamo")
    public void testCreateFind() throws Exception {
        for (int i = 0; i < 2; i++) {
            String tenantId = i + "";
            for (int j = 0; j < 2; j++) {
                String accountId = UUID.randomUUID().toString();
                Account account = new Account();
                account.setId(tenantId, "account", accountId);
                accountMgr.create(account);
                for (int k = 0; k < 2; k++) {
                    String contactId = UUID.randomUUID().toString();
                    Contact contact = new Contact();
                    contact.setId(accountId, "contact", contactId);
                    contactMgr.create(contact);
                    for (int year = 2011; year < 2013; year++) {
                        for (int month = 1; month <= 3; month++) {
                            for (int day = 0; day < 4; day++) {
                                History history = new History();
                                history.setId(contactId, "history", year + "", month +"", day+"");
                                historyMgr.create(history);
                            }
                        }
                    }
                }
            }
        }

        for (int i = 0; i < 2; i++) {
            String tenantId = i + "";
            List<CompositeGraphEntity> accountGraph = graphMgr.findChildren(tenantId);
            Assert.assertEquals(accountGraph.size(), 2);
            for (CompositeGraphEntity account : accountGraph) {
                 CompositeFabricEntity accountEntity = account.getEntity();
                 log.info("Find account " + accountEntity.getId() + " parent " + accountEntity.getParentKey() +
                          " entity " + accountEntity.getEntityId());
                 String accountParentId = account.getEntity().getParentId();
                 Assert.assertEquals(accountParentId, tenantId);
                 String accountId = account.getEntity().getEntityId();
                 Collection<CompositeGraphEntity> contacts = account.getChild("contact");
                 Assert.assertEquals(contacts.size(), 2);
                 for (CompositeGraphEntity contact : contacts) {
                     String contactParentId = contact.getEntity().getParentId();
                     Assert.assertEquals(contactParentId, accountId);
                     String contactId = contact.getEntity().getEntityId();
                     Collection<CompositeGraphEntity> histories = contact.getChild("history");
                     for (CompositeGraphEntity historyGraph : histories) {
                         History history = (History)historyGraph.getEntity();
                         String historyParentId = history.getParentId();
                         Assert.assertEquals(historyParentId, contactId);
                         log.info("history " + history.getEntityId() +
                                  " bucket " + history.getBucket() +
                                  " stamp " + history.getStamp());
                     }
                     Assert.assertEquals(histories.size(), 2 * 3 * 4);
                 }
             }
         }
    }

    class AccountEntityMgr extends CompositeFabricEntityMgrImpl<Account> {
         AccountEntityMgr(BaseFabricEntityMgrImpl.Builder builder) {
             super(builder);
         }
    }
    class ContactEntityMgr extends CompositeFabricEntityMgrImpl<Contact> {
         ContactEntityMgr(BaseFabricEntityMgrImpl.Builder builder) {
             super(builder);
         }
    }
    class HistoryEntityMgr extends TimeSeriesFabricEntityMgrImpl<History> {
         HistoryEntityMgr(BaseFabricEntityMgrImpl.Builder builder) {
             super(builder);
         }
    }
}
