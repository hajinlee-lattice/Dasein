package com.latticeengines.apps.cdl.end2end;

import static com.latticeengines.domain.exposed.cdl.S3ImportSystem.SystemType.Website;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;

public class ProcessWebVisitDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    static final String CHECK_POINT = "webvisit";

    private static final Logger log = LoggerFactory.getLogger(ProcessWebVisitDeploymentTestNG.class);

    private String customerSpace;

    @Value("${cdl.test.import.filename.webvisit}")
    private String webVisitCSV;

    @Inject
    private S3ImportSystemService s3ImportSystemService;

    @Inject
    private ActivityStoreProxy activityStoreProxy;

    private String templateFeedType;
    private DataFeedTask webVisitStreamTask;

    @BeforeClass(groups = "end2end")
    @Override
    public void setup() throws Exception {
        setupEnd2EndTestEnvironment();
        customerSpace = CustomerSpace.parse(mainCustomerSpace).getTenantId();
        log.info("Setup Complete!");
    }

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        createWebVisitTemplate();
        createSourceMediumTemplate();
        createWebVisitPatternTemplate();
        importData();
        initialVersion = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
        if (isLocalEnvironment()) {
            processAnalyzeSkipPublishToS3();
        }
        if (isLocalEnvironment()) {
            saveCheckpoint(saveToCheckPoint(), true);
        }

    }

    protected void importData() throws InterruptedException {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        mockCSVImport(BusinessEntity.Account, 1, "DefaultSystem_AccountData");
        Thread.sleep(1100);
        mockCSVImport(BusinessEntity.Contact, 1, "DefaultSystem_ContactData");
        Thread.sleep(1100);
        mockCSVImport(BusinessEntity.Product, 1, "ProductBundle");
        Thread.sleep(1100);
        mockCSVImport(BusinessEntity.Product, 2, "ProductHierarchy");
        Thread.sleep(1100);
        mockCSVImport(BusinessEntity.Product, 3, "ProductVDB");
        Thread.sleep(2000);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    public void createWebVisitTemplate() {
        EntityType type = EntityType.WebVisit;
        SimpleTemplateMetadata metadata = prepareMetadata(type,
                Arrays.asList(InterfaceName.City.name(), InterfaceName.WebVisitPageUrl.name()));
        webVisitStreamTask = createTemplate(metadata, type);

        // verification
        Assert.assertNotNull(webVisitStreamTask);
        templateFeedType = webVisitStreamTask.getFeedType();
        Table template = webVisitStreamTask.getImportTemplate();
        Assert.assertNotNull(template);
        Assert.assertNotNull(template.getAttribute(InterfaceName.WebVisitPageUrl));
        Assert.assertNull(template.getAttribute(InterfaceName.City));
        Assert.assertNotNull(template.getAttribute("user_CustomerAttr1"));
        Attribute attribute = template.getAttribute(InterfaceName.CompanyName);
        Assert.assertEquals(attribute.getDisplayName(), "CustomerName");

        // verify stream is created correctly
        verifyWebVisitStream(webVisitStreamTask, InterfaceName.PathPatternId.name(), false);
        verifyWebVisitStream(webVisitStreamTask, InterfaceName.SourceMediumId.name(), false);
    }

    public void createSourceMediumTemplate() {
        EntityType type = EntityType.WebVisitSourceMedium;
        SimpleTemplateMetadata metadata = new SimpleTemplateMetadata();
        metadata.setEntityType(type);
        List<SimpleTemplateMetadata.SimpleTemplateAttribute> standardList = new ArrayList<>();
        SimpleTemplateMetadata.SimpleTemplateAttribute standardAttr = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        standardAttr.setDisplayName("RandomName_xyz");
        standardAttr.setName(InterfaceName.SourceMedium.name());
        standardList.add(standardAttr);
        metadata.setStandardAttributes(standardList);

        DataFeedTask webVisitSourceMediumTask = createTemplate(metadata, type);

        // verification
        Assert.assertNotNull(webVisitSourceMediumTask);
        templateFeedType = webVisitSourceMediumTask.getFeedType();
        Table template = webVisitSourceMediumTask.getImportTemplate();
        Assert.assertNotNull(template);
        Assert.assertEquals(template.getAttributes().size(), 1);
        Attribute attrSourceMedium = template.getAttribute(InterfaceName.SourceMedium);
        Assert.assertNotNull(attrSourceMedium);
        Assert.assertEquals(attrSourceMedium.getDisplayName(), "RandomName_xyz");

        // only source medium catalog is attached
        verifyWebVisitStream(webVisitStreamTask, InterfaceName.PathPatternId.name(), false);
        verifyWebVisitStream(webVisitStreamTask, InterfaceName.SourceMediumId.name(), true);
    }

    private void createWebVisitPatternTemplate() {
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

        // verify both catalogs are attached to stream dimension
        verifyWebVisitStream(webVisitStreamTask, InterfaceName.PathPatternId.name(), true);
        verifyWebVisitStream(webVisitStreamTask, InterfaceName.SourceMediumId.name(), true);
    }

    private void verifyWebVisitStream(@NotNull DataFeedTask dataFeedTask, @NotNull String dimensionName,
                                      boolean catalogAttached) {
        AtlasStream stream = activityStoreProxy.findStreamByName(mainCustomerSpace, EntityType.WebVisit.name(), true);
        Assert.assertNotNull(stream);
        Assert.assertEquals(stream.getName(), EntityType.WebVisit.name());
        Assert.assertNotNull(stream.getDataFeedTaskUniqueId());
        Assert.assertEquals(stream.getDataFeedTaskUniqueId(), dataFeedTask.getUniqueId());
        // verify dimension
        Assert.assertNotNull(stream.getDimensions());
        Assert.assertEquals(stream.getDimensions().size(), 3);
        Optional<StreamDimension> result = stream.getDimensions() //
                .stream() //
                .filter(Objects::nonNull) //
                .filter(dim -> dimensionName.equals(dim.getName())) //
                .findFirst();
        Assert.assertTrue(result.isPresent(), String.format("Dimensions should contain %s dimension", dimensionName));
        StreamDimension dimension = result.get();
        Assert.assertNotNull(dimension);
        Assert.assertEquals(dimension.getName(), dimensionName);
        if (catalogAttached) {
            Assert.assertNotNull(dimension.getCatalog());
        } else {
            Assert.assertNull(dimension.getCatalog(),
                    "Should not have any catalog attached to dimension before template is created");
        }
    }

    private DataFeedTask createTemplate(SimpleTemplateMetadata metadata, EntityType type) {
        boolean result = cdlProxy.createWebVisitTemplate2(mainCustomerSpace, Collections.singletonList(metadata));
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

    private String saveToCheckPoint() {
        return CHECK_POINT;
    }
}
