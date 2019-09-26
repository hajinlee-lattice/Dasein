package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.domain.exposed.cdl.S3ImportSystem.SystemType.Website;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;
import com.latticeengines.domain.exposed.util.S3PathBuilder;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;

public class DataFeedTaskTemplateServiceImplDeploymentTestNG extends CDLDeploymentTestNGBase {

    @Inject
    private CDLProxy cdlProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private S3Service s3Service;

    @Inject
    private ActivityStoreProxy activityStoreProxy;

    private String templateFeedType;

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @AfterClass(groups = "deployment-app")
    private void tearDown() {
        testBed.deleteTenant(mainTestTenant);
    }

    @Test(groups = "deployment-app")
    public void testCreateWebVisitTemplate() {
        EntityType type = EntityType.WebVisit;
        SimpleTemplateMetadata metadata = prepareMetadata(type,
                Arrays.asList(InterfaceName.City.name(), InterfaceName.WebVisitPageUrl.name()));
        DataFeedTask dataFeedTask = createTemplate(metadata, type);

        // verification
        Assert.assertNotNull(dataFeedTask);
        templateFeedType = dataFeedTask.getFeedType();
        Table template = dataFeedTask.getImportTemplate();
        Assert.assertNotNull(template);
        Assert.assertNotNull(template.getAttribute(InterfaceName.WebVisitPageUrl));
        Assert.assertNull(template.getAttribute(InterfaceName.City));
        Assert.assertNotNull(template.getAttribute("user_CustomerAttr1"));
        Attribute attribute = template.getAttribute(InterfaceName.CompanyName);
        Assert.assertEquals(attribute.getDisplayName(), "CustomerName");
    }

    @Test(groups = "deployment-app")
    private void testCreateWebVisitPatternTemplate() {
        EntityType type = EntityType.WebVisitPathPattern;
        SimpleTemplateMetadata metadata = prepareMetadata(type,
                Collections.singletonList(InterfaceName.PathPatternName.name()));
        DataFeedTask dataFeedTask = createTemplate(metadata, type);

        // verify template
        Assert.assertNotNull(dataFeedTask);
        Table template = dataFeedTask.getImportTemplate();
        Assert.assertNotNull(template);
        // standard attribute
        Assert.assertNotNull(template.getAttribute(InterfaceName.PathPattern));
        // ignored attribute
        Assert.assertNull(template.getAttribute(InterfaceName.PathPatternName));
        // WebVisit's standard attribute
        Assert.assertNull(template.getAttribute(InterfaceName.WebVisitPageUrl));

        // verify catalog is created correctly
        Catalog catalog = activityStoreProxy.findCatalogByName(mainCustomerSpace, type.name());
        Assert.assertNotNull(catalog);
        Assert.assertNotNull(catalog.getDataFeedTask());
        Assert.assertEquals(catalog.getDataFeedTask().getUniqueId(), dataFeedTask.getUniqueId());
    }

    private DataFeedTask createTemplate(SimpleTemplateMetadata metadata, EntityType type) {
        boolean result = cdlProxy.createWebVisitTemplate(mainCustomerSpace, metadata);
        Assert.assertTrue(result);
        List<S3ImportSystem> allSystems = cdlProxy.getS3ImportSystemList(mainCustomerSpace);
        S3ImportSystem webSite = allSystems.stream() //
                .filter(system -> Website.equals(system.getSystemType())) //
                .findAny() //
                .orElse(null);
        Assert.assertNotNull(webSite,
                String.format("Should exist a website system. systems=%s", JsonUtils.serialize(allSystems)));
        String feedType = EntityTypeUtils.generateFullFeedType(webSite.getName(), type);
        return dataFeedProxy.getDataFeedTask(mainCustomerSpace, "File", feedType);
    }

    private SimpleTemplateMetadata prepareMetadata(EntityType type, List<String> ignoredStandardAttrs) {
        SimpleTemplateMetadata metadata = new SimpleTemplateMetadata();
        metadata.setEntityType(type);
        // standard attrs
        List<SimpleTemplateMetadata.SimpleTemplateAttribute> standardList = new ArrayList<>();
        SimpleTemplateMetadata.SimpleTemplateAttribute standardAttr = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        standardAttr.setDisplayName("CustomerName");
        standardAttr.setName(InterfaceName.CompanyName.name());
        standardList.add(standardAttr);
        // custom attrs
        List<SimpleTemplateMetadata.SimpleTemplateAttribute> customerList = new ArrayList<>();
        SimpleTemplateMetadata.SimpleTemplateAttribute customerAttr1 = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        customerAttr1.setDisplayName("CustomerAttr1");
        customerAttr1.setName("CustomerAttr1");
        customerAttr1.setPhysicalDataType(Schema.Type.INT);
        customerList.add(customerAttr1);

        metadata.setIgnoredStandardAttributes(new HashSet<>(ignoredStandardAttrs));
        metadata.setCustomerAttributes(customerList);
        metadata.setStandardAttributes(standardList);
        return metadata;
    }

    @Test(groups = "manual", dependsOnMethods = "testCreateWebVisitTemplate")
    public void testS3Import() {
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(mainCustomerSpace);
        Assert.assertNotNull(dropBoxSummary);
        String path = S3PathBuilder.getUiDisplayS3Dir(dropBoxSummary.getBucket(), dropBoxSummary.getDropBox(),
                templateFeedType);
        Assert.assertNotNull(path);
        String key = path.substring(dropBoxSummary.getBucket().length() + 1) + "TestWebVisit.csv";
        InputStream testFile = ClassLoader.getSystemResourceAsStream("service/impl/cdlImportWebVisit.csv");
        s3Service.uploadInputStream(dropBoxSummary.getBucket(), key, testFile, true);

    }

}
