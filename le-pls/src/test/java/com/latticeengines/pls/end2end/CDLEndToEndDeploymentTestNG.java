package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentPropertyName;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BucketRange;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import edu.emory.mathcs.backport.java.util.Collections;

public class CDLEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    private MetadataSegment segment;

    private DataCollection collection;

    private FrontEndRestriction restriction;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
    }

    @Test(groups = "deployment")
    public void checkEnvironment() {
        collection = dataCollectionProxy.getDataCollectionByType(mainTestTenant.getId(),
                DataCollectionType.Segmentation);
        assertNotNull(collection);
        assertEquals(collection.getTables().size(), 1);
    }

    @Test(groups = "deployment", dependsOnMethods = "checkEnvironment")
    public void createSegment() {
        segment = new MetadataSegment();
        segment.setDisplayName("Test");
        segment.setName("Test");
        segment = restTemplate.postForObject(String.format("%s/pls/metadatasegments/", getRestAPIHostPort()), segment,
                MetadataSegment.class);
        assertEquals((long) segment.getSegmentPropertyBag()
                .get(MetadataSegmentPropertyName.NumAccounts, 0L, Long.class), 335045841);
    }

    @Test(groups = "deployment", dependsOnMethods = "createSegment")
    public void getNumAccountsForSegment() {
        FrontEndRestriction restriction = getArbitraryRestriction();

        long count = restTemplate.postForObject(
                String.format("%s/pls/accounts/count/restriction", getRestAPIHostPort()), restriction, Long.class);
        assertEquals(count, 0);
    }

    @Test(groups = "deployment", dependsOnMethods = "getNumAccountsForSegment")
    public void viewAccountsForSegment() {
        FrontEndRestriction restriction = getArbitraryRestriction();
        FrontEndQuery query = new FrontEndQuery();
        query.setRestriction(restriction);
        query.setPageFilter(new PageFilter(0, 50));
        DataPage page = restTemplate.postForObject(String.format("%s/pls/accounts/data/", getRestAPIHostPort()), query,
                DataPage.class);
    }

    @Test(groups = "deployment", dependsOnMethods = "viewAccountsForSegment")
    public void modifySegment() {
        segment.setSimpleRestriction(getArbitraryRestriction());
        segment = restTemplate.postForObject(String.format("%s/pls/metadatasegments/", getRestAPIHostPort()), segment,
                MetadataSegment.class);
    }

    @SuppressWarnings("unchecked")
    private FrontEndRestriction getArbitraryRestriction() {
        if (restriction != null) {
            return restriction;
        }

        AccountMasterCube cube = restTemplate.getForObject(
                String.format("%s/pls/latticeinsights/stats/cube", getRestAPIHostPort()), AccountMasterCube.class);
        List<Bucket> buckets = cube.getStatistics().get("TechIndicator_AdRoll").getRowBasedStatistics().getBuckets()
                .getBucketList();

        restriction = new FrontEndRestriction();
        BucketRange range = buckets.get(0).getRange();
        // TODO Temporary hack fix
        range.setMin(StringUtils.capitalize(range.getMin().toString().toLowerCase()));
        range.setMax(StringUtils.capitalize(range.getMax().toString().toLowerCase()));
        BucketRestriction bucketRestriction = new BucketRestriction(new ColumnLookup(
                SchemaInterpretation.BucketedAccountMaster, "TechIndicator_AdRoll"), range);
        restriction.setAll(Collections.singletonList(bucketRestriction));
        return restriction;
    }
}
