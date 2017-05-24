package com.latticeengines.pls.setup;

import java.io.File;
import java.util.Random;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeStatistics;
import com.latticeengines.domain.exposed.metadata.statistics.CategoryStatistics;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.metadata.statistics.SubcategoryStatistics;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BucketRange;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.security.exposed.service.TenantService;

@Component
public class CDLTestSetupTestNG extends PlsDeploymentTestNGBase {

    @Autowired
    private TenantService tenantService;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;
    private Random random = new Random();

    @Test(groups = "deployment")
    public void setupTenant() {
        setupTenant(CustomerSpace.parse("CDLTest"));
    }

    public void setupTenant(CustomerSpace customerSpace) {
        if (!tenantService.hasTenantId(customerSpace.toString())) {
            createTenant(customerSpace);
        }

        DataCollection dataCollection = dataCollectionProxy.getDataCollectionByType(customerSpace.toString(),
                DataCollectionType.Segmentation);
        StatisticsContainer container = new StatisticsContainer();
        container.setStatistics(generateStatistics());
        dataCollection.setStatisticsContainer(container);
        dataCollectionProxy.createOrUpdateDataCollection(customerSpace.toString(), dataCollection);
    }

    private Statistics generateStatistics() {
        File currPath = new File(System.getProperty("user.dir"));
        File file = new File(currPath.getParentFile().getAbsolutePath()
                + "/le-dev/testartifacts/AccountMaster/AccountMasterBucketed.avsc");
        Configuration config = new Configuration();
        config.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        Table bucketedTable = MetadataConverter.getBucketedTableFromSchemaPath(config, file.getPath(), null, null);

        Statistics statistics = new Statistics();

        for (Attribute attribute : bucketedTable.getAttributes()) {
            if (attribute.getCategory() == null) {
                attribute.setCategory(Category.DEFAULT);
            }
            if (attribute.getSubcategory() == null) {
                attribute.setSubcategory(ColumnMetadata.SUBCATEGORY_OTHER);
            }

            if (!statistics.getCategories().containsKey(attribute.getCategory())) {
                statistics.getCategories().put(attribute.getCategory(), new CategoryStatistics());
            }
            CategoryStatistics categoryStatistics = statistics.getCategories().get(attribute.getCategory());
            if (!categoryStatistics.getSubcategories().containsKey(attribute.getSubcategory())) {
                categoryStatistics.getSubcategories().put(attribute.getSubcategory(), new SubcategoryStatistics());
            }
            SubcategoryStatistics subcategoryStatistics = categoryStatistics.getSubcategories().get(
                    attribute.getSubcategory());
            AttributeStatistics attributeStatistics = generateAttributeStatistics(attribute);
            subcategoryStatistics.getAttributes().put(
                    new ColumnLookup(SchemaInterpretation.BucketedAccountMaster, attribute.getName()),
                    attributeStatistics);
        }

        return statistics;
    }

    private AttributeStatistics generateAttributeStatistics(Attribute attribute) {
        AttributeStatistics attributeStatistics = new AttributeStatistics();
        if (attribute.getBucketRangeList() != null) {
            for (BucketRange range : attribute.getBucketRangeList()) {
                if (range != null) {
                    attributeStatistics.getBuckets().add(generateBucket(range));
                }
            }
        }
        return attributeStatistics;
    }

    private Bucket generateBucket(BucketRange range) {
        Bucket bucket = new Bucket();
        bucket.setBucketLabel(ObjectUtils.toString(range.getMax()));
        bucket.setRange(range);
        bucket.setCount((long) random.nextInt(100000));
        bucket.setLift(random.nextInt(500) / 100.0);
        return bucket;
    }

    private void createTenant(CustomerSpace customerSpace) {
        Tenant tenant = new Tenant();
        tenant.setId(customerSpace.toString());
        tenant.setName(customerSpace.toString());
        magicRestTemplate.postForObject(getDeployedRestAPIHostPort() + "/pls/admin/tenants", tenant, Boolean.class);
    }
}
