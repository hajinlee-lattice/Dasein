package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;

import java.util.Collections;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentPropertyName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BucketRange;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class CDLEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Autowired
    private MetadataProxy metadataProxy;

    private MetadataSegment segment;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
        setupDataCollection();
    }

    private void setupDataCollection() {
        Table table = createTestTable();
        metadataProxy.createTable(mainTestTenant.getId(), table.getName(), table);
        DataCollection collection = new DataCollection();
        collection.setType(DataCollectionType.Segmentation);
        collection.setTables(Collections.singletonList(table));
        metadataProxy.createDataCollection(mainTestTenant.getId(), collection);
    }

    @Test(groups = "deployment")
    public void createSegment() {
        segment = new MetadataSegment();
        segment.setDisplayName("Test");
        segment.setName("Test");
        segment = restTemplate.postForObject(String.format("%s/pls/metadatasegments/", getRestAPIHostPort()), segment,
                MetadataSegment.class);
        assertEquals((long) segment.getSegmentPropertyBag()
                .get(MetadataSegmentPropertyName.NumAccounts, 0L, Long.class), 611136);
    }

    @Test(groups = "deployment", dependsOnMethods = "createSegment")
    public void getNumAccountsForSegment() {
        FrontEndRestriction restriction = getArbitraryRestriction();

        long count = restTemplate.postForObject(
                String.format("%s/pls/accounts/count/restriction", getRestAPIHostPort()), restriction, Long.class);
        assertEquals(count, 5);
    }

    @Test(groups = "deployment", dependsOnMethods = "getNumAccountsForSegment")
    public void viewAccountsForSegment() {
        FrontEndRestriction restriction = getArbitraryRestriction();
        FrontEndQuery query = new FrontEndQuery();
        query.setRestriction(restriction);
        query.setPageFilter(new PageFilter(0, 50));
        DataPage page = restTemplate.postForObject(String.format("%s/pls/accounts/data/", getRestAPIHostPort()), query,
                DataPage.class);
        assertEquals(page.getData().size(), 5);
    }

    @Test(groups = "deployment", dependsOnMethods = "viewAccountsForSegment")
    public void modifySegment() {
        segment.setSimpleRestriction(getArbitraryRestriction());
        segment = restTemplate.postForObject(String.format("%s/pls/metadatasegments/", getRestAPIHostPort()), segment,
                MetadataSegment.class);
        assertEquals((long) segment.getSegmentPropertyBag()
                .get(MetadataSegmentPropertyName.NumAccounts, 0L, Long.class), 5);
    }

    private FrontEndRestriction getArbitraryRestriction() {
        FrontEndRestriction restriction = new FrontEndRestriction();
        BucketRange bucket = new BucketRange(1);
        BucketRestriction bucketRestriction = new BucketRestriction(new ColumnLookup("number_of_family_members"),
                bucket);
        restriction.setAll(Collections.singletonList(bucketRestriction));
        segment.setSimpleRestriction(restriction);
        return restriction;
    }

    private Table createTestTable() {
        Table table = new Table();
        table.setInterpretation(SchemaInterpretation.Account.toString());
        table.setName("querytest_table");
        table.setDisplayName("querytest_table");
        Attribute companyName = createAttribute("companyname");
        table.addAttribute(companyName);
        Attribute city = createAttribute("city");
        table.addAttribute(city);
        Attribute state = createAttribute("state");
        table.addAttribute(state);
        Attribute lastName = createAttribute("lastname");
        table.addAttribute(lastName);
        Attribute familyMembers = createAttribute("number_of_family_members");
        table.addAttribute(familyMembers);
        JdbcStorage storage = new JdbcStorage();
        storage.setTableNameInStorage("querytest_table");
        storage.setDatabaseName(JdbcStorage.DatabaseName.REDSHIFT);
        table.setStorageMechanism(storage);
        return table;
    }

    private Attribute createAttribute(String name) {
        Attribute attribute = new Attribute();
        attribute.setName(name);
        attribute.setDisplayName(name);
        attribute.setPhysicalDataType("String");
        return attribute;
    }
}
