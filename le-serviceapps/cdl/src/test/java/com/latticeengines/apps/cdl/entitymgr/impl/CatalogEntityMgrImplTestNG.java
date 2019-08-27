package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.persistence.PersistenceException;

import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.CatalogEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.security.TenantType;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({ SimpleRetryListener.class })
public class CatalogEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CatalogEntityMgrImplTestNG.class);

    private static final String WEB_VISIT = "WebVisit";
    private static final String ACTIVITY = "Activity";
    private static final String PRODUCT = "Product";
    private static final List<String> CATALOG_NAMES = Arrays.asList(WEB_VISIT, ACTIVITY, PRODUCT);

    @Inject
    private DataFeedEntityMgr datafeedEntityMgr;

    @Inject
    private CatalogEntityMgr catalogEntityMgr;

    /*-
     * test objects
     */
    private DataFeed feed;
    private DataFeedTask task;
    private Table importTemplate;
    // catalog name -> catalog
    private Map<String, Catalog> catalogs = new HashMap<>();

    @BeforeClass(groups = "functional")
    private void setup() {
        setupTestEnvironmentWithDataCollection();
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(mainCustomerSpace));
    }

    @Test(groups = "functional")
    private void testCreate() {
        prepareDataFeed();

        for (String name : CATALOG_NAMES) {
            Catalog catalog = new Catalog();
            catalog.setTenant(mainTestTenant);
            catalog.setDataFeedTask(task);
            catalog.setName(name);
            catalogEntityMgr.create(catalog);

            Assert.assertNotNull(catalog.getPid(), "PID should not be null");

            // put into map to test reads
            catalogs.put(name, catalog);
        }
        Assert.assertEquals(catalogs.size(), CATALOG_NAMES.size());
    }

    /*-
     * [ Name + DataFeedTask ] need to be unique
     */
    @Test(groups = "functional", dependsOnMethods = "testCreate", expectedExceptions = { PersistenceException.class })
    private void testCreateConflict() {
        // all of these catalog names should already be created
        String name = CATALOG_NAMES.get(RandomUtils.nextInt(0, CATALOG_NAMES.size()));
        Catalog catalog = new Catalog();
        catalog.setTenant(mainTestTenant);
        catalog.setDataFeedTask(task);
        catalog.setName(name);
        catalogEntityMgr.create(catalog);
    }

    @Test(groups = "functional", retryAnalyzer = SimpleRetryListener.class, dependsOnMethods = "testCreate")
    private void testFindByNameAndTenant() {
        Tenant notExistTenant = notExistTenant();
        for (String name : CATALOG_NAMES) {
            log.info("Querying Catalog {} for tenant {}", name, mainCustomerSpace);
            // use existing tenant + valid name
            List<Catalog> results = catalogEntityMgr.findByNameAndTenant(name, mainTestTenant);
            Assert.assertNotNull(results);
            Assert.assertEquals(results.size(), 1,
                    String.format("Should only have one catalog named %s for tenant %s", name, mainCustomerSpace));
            assertEqual(results.get(0), catalogs.get(name));

            List<Catalog> notExistingCatalogs = catalogEntityMgr.findByNameAndTenant(name, notExistTenant);
            Assert.assertNotNull(notExistingCatalogs);
            Assert.assertTrue(notExistingCatalogs.isEmpty(),
                    String.format("Querying catalog %s for tenant %s should get no result, got %s instead", name,
                            notExistTenant.getId(), JsonUtils.serialize(notExistingCatalogs)));

        }

        // existing tenant + invalid name
        String notExistingCatalogName = NamingUtils.uuid("not_existing_catalog");
        List<Catalog> notExistingCatalogs = catalogEntityMgr.findByNameAndTenant(notExistingCatalogName, mainTestTenant);
        Assert.assertNotNull(notExistingCatalogs, String.format("Should not get null list query catalog %s for tenant %s", notExistingCatalogName, mainCustomerSpace));
        Assert.assertTrue(notExistingCatalogs.isEmpty(),
                String.format("Querying catalog %s for tenant %s should get no result, got %s instead", notExistingCatalogName,
                        notExistTenant.getId(), JsonUtils.serialize(notExistingCatalogs)));
    }

    @Test(groups = "functional", retryAnalyzer = SimpleRetryListener.class, dependsOnMethods = "testCreate")
    private void testFindByTenant() {
        List<Catalog> results = catalogEntityMgr.findByTenant(mainTestTenant);
        Assert.assertNotNull(results);

        // make sure no duplicate
        Map<String, Catalog> catalogMap = results.stream()
                .collect(Collectors.toMap(Catalog::getName, catalog -> catalog, (c1, c2) -> c1));
        Assert.assertEquals(catalogMap.size(), catalogs.size());
        // verify individual catalogs
        catalogMap.values().forEach(catalog -> assertEqual(catalog, catalogs.get(catalog.getName())));
    }

    // TODO multiple catalog with the same name & diff DataFeedTask

    private void assertEqual(Catalog catalog, Catalog expectedCatalog) {
        if (expectedCatalog == null) {
            Assert.assertNull(catalog);
            return;
        }

        // catalog field verification
        Assert.assertNotNull(catalog);
        Assert.assertEquals(catalog.getPid(), expectedCatalog.getPid());
        Assert.assertEquals(catalog.getName(), expectedCatalog.getName());
        Assert.assertNotNull(catalog.getCreated());
        Assert.assertNotNull(catalog.getUpdated());

        // datafeed task verification
        Assert.assertNotNull(catalog.getDataFeedTask());
        DataFeedTask dataFeedTask = catalog.getDataFeedTask();
        Assert.assertEquals(dataFeedTask.getPid(), task.getPid());
        // datafeed verification
        Assert.assertNotNull(dataFeedTask.getDataFeed(), "Datafeed should not be null");
        Assert.assertEquals(dataFeedTask.getDataFeed().getPid(), feed.getPid());
        Assert.assertEquals(dataFeedTask.getDataFeed().getName(), feed.getName());
        // import template verification
        Assert.assertNotNull(dataFeedTask.getImportTemplate(), "Import template should not be null");
        Assert.assertEquals(dataFeedTask.getImportTemplate().getPid(), importTemplate.getPid());
        Assert.assertEquals(dataFeedTask.getImportTemplate().getName(), importTemplate.getName());
    }

    /*-
     * test data helpers
     */

    private Tenant notExistTenant() {
        Tenant tenant = new Tenant(getClass().getSimpleName() + "_" + UUID.randomUUID().toString());
        tenant.setPid(-1L);
        tenant.setTenantType(TenantType.QA);
        tenant.setStatus(TenantStatus.ACTIVE);
        tenant.setUiVersion("4.0");
        return tenant;
    }

    private void prepareDataFeed() {
        feed = testDataFeed();
        task = testFeedTask();
        importTemplate = testImportTemplate();
        task.setDataFeed(feed);
        feed.addTask(task);
        task.setImportTemplate(importTemplate);

        datafeedEntityMgr.create(feed);
    }

    private DataFeed testDataFeed() {
        DataFeed feed = new DataFeed();
        feed.setName(name());
        feed.setStatus(DataFeed.Status.Active);
        feed.setDataCollection(dataCollection);
        return feed;
    }

    private DataFeedTask testFeedTask() {
        DataFeedTask task = new DataFeedTask();
        task.setActiveJob("Not specified");
        task.setEntity(BusinessEntity.Account.name());
        task.setSource("SFDC");
        task.setStatus(DataFeedTask.Status.Active);
        task.setSourceConfig("config");
        task.setStartTime(new Date());
        task.setLastImported(new Date());
        task.setLastUpdated(new Date());
        task.setUniqueId(name());
        return task;
    }

    private Table testImportTemplate() {
        Table table = new Table();
        table.setName(name());
        table.setDisplayName(table.getName());
        table.setTenant(mainTestTenant);
        Attribute attr = new Attribute();
        attr.setName(name());
        attr.setDisplayName(attr.getName());
        attr.setPhysicalDataType(String.class.getName());
        table.addAttribute(attr);
        return table;
    }

    private String name() {
        return NamingUtils.timestamp(getClass().getSimpleName());
    }
}
