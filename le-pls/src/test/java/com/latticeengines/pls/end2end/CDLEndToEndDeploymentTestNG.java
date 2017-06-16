package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentPropertyName;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeStatistics;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BucketRange;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.pls.functionalframework.CDLDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;


public class CDLEndToEndDeploymentTestNG extends CDLDeploymentTestNGBase {

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    private MetadataSegment segment;

    private FrontEndRestriction arbitraryRestriction;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTenant();
        // initial verification
        verifyMasterStats();
    }

    @Test(groups = "deployment", enabled = false)
    public void createSegment() {
        segment = new MetadataSegment();
        segment.setDisplayName("Test");
        segment.setName("Test");
        segment = restTemplate.postForObject(String.format("%s/pls/metadatasegments/", getRestAPIHostPort()), segment,
                MetadataSegment.class);
        assertEquals((long) segment.getSegmentPropertyBag()
                .get(MetadataSegmentPropertyName.NumAccounts, 0L, Long.class), 335045841);
    }

    @Test(groups = "deployment", dependsOnMethods = "createSegment", enabled = false)
    public void getNumAccountsForSegment() {
        FrontEndRestriction restriction = getArbitraryRestriction();

        long count = restTemplate.postForObject(
                String.format("%s/pls/accounts/count/restriction", getRestAPIHostPort()), restriction, Long.class);
        assertEquals(count, 334904662);
    }

    @Test(groups = "deployment", dependsOnMethods = "getNumAccountsForSegment", enabled = false)
    public void viewAccountsForSegment() {
        FrontEndQuery query = new FrontEndQuery();
        query.setPageFilter(new PageFilter(0, 50));
        DataPage page = restTemplate.postForObject(String.format("%s/pls/accounts/data/", getRestAPIHostPort()), query,
                DataPage.class);
        assertEquals(page.getData().size(), 50);
    }

    @Test(groups = "deployment", dependsOnMethods = "viewAccountsForSegment", enabled = false)
    public void viewAccountsForSegmentWithFreeFormSearch() {
        FrontEndQuery query = new FrontEndQuery();
        query.setPageFilter(new PageFilter(0, 50));
        query.setFreeFormTextSearch("a");
        DataPage page = restTemplate.postForObject(String.format("%s/pls/accounts/data/", getRestAPIHostPort()), query,
                DataPage.class);
        assertEquals(page.getData().size(), 50);

        long count = restTemplate.postForObject(String.format("%s/pls/accounts/count", getRestAPIHostPort()), query,
                Long.class);
        assertTrue(count < 335045841);
    }

    @Test(groups = "deployment", dependsOnMethods = "viewAccountsForSegmentWithFreeFormSearch", enabled = false)
    public void modifySegment() {
        segment.setSimpleRestriction(getArbitraryRestriction());
        segment = restTemplate.postForObject(String.format("%s/pls/metadatasegments/", getRestAPIHostPort()), segment,
                MetadataSegment.class);
        assertEquals(segment.getAttributeDependencies().size(), 1);
    }

    @SuppressWarnings("unchecked")
    private FrontEndRestriction getArbitraryRestriction() {
        if (arbitraryRestriction != null) {
            return arbitraryRestriction;
        }

        Statistics statistics = restTemplate.getForObject(
                String.format("%s/pls/metadata/statistics", getRestAPIHostPort()), Statistics.class);
        AttributeStatistics attributeStatistics = statistics //
                .getCategories().get(Category.DEFAULT.toString()) //
                .getSubcategories().get(ColumnMetadata.SUBCATEGORY_OTHER).getAttributes() //
                .get(new ColumnLookup(SchemaInterpretation.AccountMaster, "TechIndicator_AdRoll"));

        arbitraryRestriction = new FrontEndRestriction();
        BucketRange range = attributeStatistics.getBuckets().get(0).getRange();
        BucketRestriction bucketRestriction = new BucketRestriction(new ColumnLookup(
                SchemaInterpretation.AccountMaster, "TechIndicator_AdRoll"), range);
        arbitraryRestriction.setAll(Collections.singletonList(bucketRestriction));
        return arbitraryRestriction;
    }

    private void verifyMasterStats() throws IOException {
        StatisticsContainer statsContainer = dataCollectionProxy.getStats(customerSpace, dataCollectionName);
        FileUtils.write(new File("stats.json"), JsonUtils.pprint(statsContainer));
    }
}
