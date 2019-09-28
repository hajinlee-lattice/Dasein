package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import com.latticeengines.apps.cdl.entitymgr.AtlasStreamEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.CatalogEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.StreamDimensionEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.security.TenantType;

public class ActivityRelatedEntityMgrImplTestNGBase extends CDLFunctionalTestNGBase {

    protected static final List<String> MATCH_ENTITIES = Arrays.asList(BusinessEntity.Account.name(),
            BusinessEntity.Contact.name());
    protected static final List<String> AGGR_ENTITIES = Arrays.asList(BusinessEntity.Account.name());
    protected static final List<PeriodStrategy.Template> PERIODS = Arrays.asList(PeriodStrategy.Template.Week,
            PeriodStrategy.Template.Month);

    @Inject
    protected DataFeedEntityMgr datafeedEntityMgr;

    @Inject
    protected CatalogEntityMgr catalogEntityMgr;

    @Inject
    protected AtlasStreamEntityMgr streamEntityMgr;

    @Inject
    protected StreamDimensionEntityMgr dimensionEntityMgr;

    protected DataFeed feed;
    // catalog name -> datafeed task
    protected Map<String, DataFeedTask> taskMap = new HashMap<>();
    protected Table importTemplate;
    // catalog name -> catalog
    protected Map<String, Catalog> catalogs = new HashMap<>();
    // stream name -> stream
    protected Map<String, AtlasStream> streams = new HashMap<>();
    protected List<StreamAttributeDeriver> attrDerivers = getAttributeDerivers();

    // Override this method to provide a list of names to create catalogs
    protected List<String> getCatalogNames() {
        return Collections.emptyList();
    };

    // Override this method to provide a list of names to create streams
    protected List<String> getStreamNames() {
        return Collections.emptyList();
    };

    protected void prepareDataFeed() {
        feed = testDataFeed();
        importTemplate = testImportTemplate();
        for (String name : getCatalogNames()) {
            DataFeedTask task = testFeedTask();
            task.setDataFeed(feed);
            feed.addTask(task);
            task.setImportTemplate(importTemplate);
            taskMap.put(name, task);
        }
        datafeedEntityMgr.create(feed);
    }

    protected void prepareCatalog() {
        for (String name : getCatalogNames()) {
            Catalog catalog = createCatalog(name);
            catalogs.put(name, catalog);
        }
    }

    protected Catalog createCatalog(String name) {
        Catalog catalog = new Catalog();
        catalog.setTenant(mainTestTenant);
        catalog.setDataFeedTask(taskMap.get(name));
        catalog.setName(name);
        catalogEntityMgr.create(catalog);
        return catalog;
    }

    protected void prepareStream() {
        for (String name : getStreamNames()) {
            AtlasStream stream = createStream(name);
            streams.put(name, stream);
        }
    }

    protected AtlasStream createStream(String name) {
        AtlasStream stream = new AtlasStream();
        stream.setName(name);
        stream.setTenant(mainTestTenant);
        stream.setMatchEntities(MATCH_ENTITIES);
        stream.setAggrEntities(AGGR_ENTITIES);
        stream.setDateAttribute(InterfaceName.WebVisitDate.name());
        stream.setPeriods(PERIODS);
        stream.setRetentionDays(1000);
        stream.setAttributeDerivers(attrDerivers);
        streamEntityMgr.create(stream);
        return stream;
    }

    // Fake some derived attributes
    private List<StreamAttributeDeriver> getAttributeDerivers() {
        StreamAttributeDeriver deriver1 = new StreamAttributeDeriver();
        deriver1.setCalculation(StreamAttributeDeriver.Calculation.COUNT);
        deriver1.setSourceAttributes(Arrays.asList(InterfaceName.ContactId.name()));
        deriver1.setTargetAttribute(InterfaceName.NumberOfContacts.name());

        StreamAttributeDeriver deriver2 = new StreamAttributeDeriver();
        deriver2.setCalculation(StreamAttributeDeriver.Calculation.SUM);
        deriver2.setSourceAttributes(Arrays.asList(InterfaceName.Amount.name()));
        deriver2.setTargetAttribute(InterfaceName.TotalAmount.name());

        return Arrays.asList(deriver1, deriver2);
    }

    protected Tenant notExistTenant() {
        Tenant tenant = new Tenant(getClass().getSimpleName() + "_" + UUID.randomUUID().toString());
        tenant.setPid(-1L);
        tenant.setTenantType(TenantType.QA);
        tenant.setStatus(TenantStatus.ACTIVE);
        tenant.setUiVersion("4.0");
        return tenant;
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
        return NamingUtils.uuid(getClass().getSimpleName());
    }
}
