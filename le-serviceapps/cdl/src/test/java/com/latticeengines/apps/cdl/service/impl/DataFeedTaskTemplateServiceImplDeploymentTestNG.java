package com.latticeengines.apps.cdl.service.impl;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;
import com.latticeengines.domain.exposed.util.S3PathBuilder;
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

    private String feedType;

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = "deployment-app")
    public void testCreateTemplate() {
        SimpleTemplateMetadata metadata = new SimpleTemplateMetadata();
        metadata.setEntityType(EntityType.WebVisit);
        List<SimpleTemplateMetadata.SimpleTemplateAttribute> standardList = new ArrayList<>();
        SimpleTemplateMetadata.SimpleTemplateAttribute standardAttr = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        standardAttr.setDisplayName("CustomerName");
        standardAttr.setName(InterfaceName.CompanyName.name());
        standardList.add(standardAttr);
        List<SimpleTemplateMetadata.SimpleTemplateAttribute> customerList = new ArrayList<>();
        SimpleTemplateMetadata.SimpleTemplateAttribute customerAttr1 =
                new SimpleTemplateMetadata.SimpleTemplateAttribute();
        customerAttr1.setDisplayName("CustomerAttr1");
        customerAttr1.setName("CustomerAttr1");
        customerAttr1.setPhysicalDataType(Schema.Type.INT);
        customerList.add(customerAttr1);
        Set<String> ignored = new HashSet<>(Arrays.asList(InterfaceName.City.name(),
                InterfaceName.WebVisitPageUrl.name()));
        metadata.setIgnoredStandardAttributes(ignored);
        metadata.setCustomerAttributes(customerList);
        metadata.setStandardAttributes(standardList);
        boolean result = cdlProxy.createWebVisitTemplate(mainCustomerSpace, metadata);
        Assert.assertTrue(result);
        List<S3ImportSystem> allSystems = cdlProxy.getS3ImportSystemList(mainCustomerSpace);
        S3ImportSystem webSite =
                allSystems.stream().filter(system -> S3ImportSystem.SystemType.Website.equals(system.getSystemType())).findAny().orElse(null);
        Assert.assertNotNull(webSite);
        feedType = EntityTypeUtils.generateFullFeedType(webSite.getName(), EntityType.WebVisit);
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(mainCustomerSpace, "File", feedType);
        Assert.assertNotNull(dataFeedTask);
        Table template = dataFeedTask.getImportTemplate();
        Assert.assertNotNull(template);
        Assert.assertNotNull(template.getAttribute(InterfaceName.WebVisitPageUrl));
        Assert.assertNull(template.getAttribute(InterfaceName.City));
        Assert.assertNotNull(template.getAttribute("user_CustomerAttr1"));
        Attribute attribute = template.getAttribute(InterfaceName.CompanyName);
        Assert.assertEquals(attribute.getDisplayName(), "CustomerName");
    }

    @Test(groups = "manual", dependsOnMethods = "testCreateTemplate")
    public void testS3Import() {
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(mainCustomerSpace);
        Assert.assertNotNull(dropBoxSummary);
        String path = S3PathBuilder.getUiDisplayS3Dir(dropBoxSummary.getBucket(), dropBoxSummary.getDropBox(),
                feedType);
        Assert.assertNotNull(path);
        String key = path.substring(dropBoxSummary.getBucket().length() + 1) + "TestWebVisit.csv";
        InputStream testFile = ClassLoader.getSystemResourceAsStream("service/impl/cdlImportWebVisit.csv");
        s3Service.uploadInputStream(dropBoxSummary.getBucket(), key, testFile, true);

    }

}
