package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.MetadataService;

public class AttributeEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AttributeEntityMgrImplTestNG.class);

    @Autowired
    private MetadataService metadataService;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @AfterClass(groups = "functional")
    public void tearDown() {
        metadataService.deleteTableAndCleanup(CustomerSpace.parse(customerSpace2), TABLE2);
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace2));
        Table t = tableEntityMgr.findByName(TABLE2);
        assertNull(t);
    }

    @Test(groups = "functional")
    public void testCountAttributes() {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace1));
        Table table = tableEntityMgr.findByName(TABLE1);
        Long attributeCnt = countByTablePid(table.getPid());
        log.info("Attribute Count for table {} - {} ", TABLE1, attributeCnt);
        assertEquals(table.getAttributes().size(), attributeCnt.intValue());
    }

    @Test(groups = "functional")
    public void testFindAttributes() {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace1));
        Table table = tableEntityMgr.findByName(TABLE1);

        List<Attribute> attributes = findByTablePid(table.getPid());
        log.info("Attribute List Size for table {} - {} ", TABLE1, attributes.size());
        assertEquals(table.getAttributes().size(), attributes.size());

        attributes = findByTablePid(table.getPid(), PageRequest.of(0, CollectionUtils.size(attributes)));
        log.info("Attribute List Size for table {} - {} ", TABLE1, attributes.size());
        assertEquals(table.getAttributes().size(), attributes.size());

        List<Attribute> paginatedAttrs = new ArrayList<>();
        IntStream.range(0, (int) Math.ceil(attributes.size() / 5.0)).forEach(page -> {
            List<Attribute> currPage = findByTablePid(table.getPid(), PageRequest.of(page, 5));
            paginatedAttrs.addAll(currPage);
            log.info("Attribute List by page {} - {} - Results: {} ", page, currPage.size(), currPage);
        });
        assertEquals(paginatedAttrs.size(), attributes.size());
    }

    @Test(groups = "functional")
    public void findByNameAndTableName() {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace1));
        validateAttributes("Id", TABLE1);
        validateAttributes("Name", TABLE1);
        validateAttributes("Type", TABLE1);
        validateAttributes("Street", TABLE1);
        validateAttributes("City", TABLE1);
        validateAttributes("State", TABLE1);
        validateAttributes("PostalCode", TABLE1);
        validateAttributes("Country", TABLE1);
        validateAttributes("Website", TABLE1);
        validateAttributes("Sic", TABLE1);
        validateAttributes("Industry", TABLE1);
        validateAttributes("AnnualRevenue", TABLE1);
        validateAttributes("NumberOfEmployees", TABLE1);
        validateAttributes("Ownership", TABLE1);
        validateAttributes("TickerSymbol", TABLE1);
        validateAttributes("Rating", TABLE1);
        validateAttributes("OwnerId", TABLE1);
        validateAttributes("CreatedDate", TABLE1);
        validateAttributes("LastModifiedDate", TABLE1);
        validateAttributes("LastActivityDate", TABLE1);
        validateAttributes("LastViewedDate", TABLE1);
        validateAttributes("AccountSource", TABLE1);
    }

    private void validateAttributes(String attributeName, String tableName) {
        List<Attribute> attributes = getAttributesByNameAndTableName(attributeName, tableName);
        assertNotNull(attributes);
        assertEquals(attributes.size(), 2);
    }

    protected List<Attribute> getAttributesByNameAndTableName(String attributeName, String tableName) {
        return attributeEntityMgr.getAttributesByNameAndTableName(attributeName, tableName);
    }

    protected long countByTablePid(Long tablePid) {
        return attributeEntityMgr.countByTablePid(tablePid);
    }

    protected List<Attribute> findByTablePid(Long tablePid) {
        return attributeEntityMgr.findByTablePid(tablePid);
    }

    protected List<Attribute> findByTablePid(Long tablePid, Pageable pageable) {
        return attributeEntityMgr.findByTablePid(tablePid, pageable);
    }
}
