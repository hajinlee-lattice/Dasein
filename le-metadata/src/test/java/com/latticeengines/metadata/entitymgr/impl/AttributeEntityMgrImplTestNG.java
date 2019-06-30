package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
import com.latticeengines.domain.exposed.security.Tenant;
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
        // set the tenant null to test method
        MultiTenantContext.setTenant(null);
        Long attributeCnt = countByTablePid(table.getPid());
        log.info("Attribute Count for table {} - {} ", TABLE1, attributeCnt);
        assertEquals(table.getAttributes().size(), attributeCnt.intValue());
    }

    @Test(groups = "functional")
    public void testFindAttributes() {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace1));
        Table table = tableEntityMgr.findByName(TABLE1);
        // set the tenant null to test method
        MultiTenantContext.setTenant(null);
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

    private List<String> getAttributeNames() {
        String[] attributeStrs = new String[]{"Id", "Name", "Type", "Street", "City", "State", "PostalCode", "Country", "Website",
                "Sic", "Industry", "AnnualRevenue", "NumberOfEmployees", "Ownership", "TickerSymbol",
                "Rating", "OwnerId", "CreatedDate", "LastModifiedDate", "LastActivityDate", "AccountSource", "LastViewedDate"};
        List<String> attributeNames = Arrays.asList(attributeStrs);
        return attributeNames;
    }

    @Test(groups = "functional")
    public void findByNameAndTableName() {
        Tenant tenant1 = tenantEntityMgr.findByTenantId(customerSpace1);
        MultiTenantContext.setTenant(tenant1);
        validateAttributes(getAttributeNames(), TABLE1, 1, tenant1);
        validateAttributes(getAttributeNames(), TABLE1, 0, tenant1);
        Tenant tenant2 = tenantEntityMgr.findByTenantId(customerSpace2);
        MultiTenantContext.setTenant(tenant2);
        validateAttributes(getAttributeNames(), TABLE1, 1, tenant2);
        validateAttributes(getAttributeNames(), TABLE1, 0, tenant2);
    }

    private void validateAttributes(List<String> attributeNames, String tableName, int tableTypeCode, Tenant tenant) {
        List<Attribute> attributes = getAttributesByNamesAndTableName(attributeNames, tableName, tableTypeCode);
        assertNotNull(attributes);
        assertEquals(attributes.size(), 22);
        // attribute name should be unique
        Set<String> attributeNameSet = new HashSet<>();
        attributes.forEach(attribute -> {
            assertEquals(attribute.getTenantId(), tenant.getPid());
            assertTrue(attributeNames.contains(attribute.getName()));
            assertEquals(attribute.getTable().getName(), tableName);
            attributeNameSet.add(attribute.getName());
        });
        assertEquals(attributeNameSet.size(), 22);
    }

    protected List<Attribute> getAttributesByNamesAndTableName(List<String> attributeNames, String tableName,
                                                               int tableTypeCode) {
        return attributeEntityMgr.getAttributesByNamesAndTableName(attributeNames, tableName, tableTypeCode);
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
