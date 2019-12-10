package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.domain.exposed.cdl.S3ImportSystem.SystemType.Website;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.datanucleus.util.StringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;
import com.latticeengines.domain.exposed.util.S3PathBuilder;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;

/*-
 * dpltc deploy -a pls,admin,cdl,lp,metadata
 */
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
    private DataFeedTask webVisitStreamTask;

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = "deployment-app")
    public void testCreateWebVisitTemplate() {
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
        Assert.assertEquals(template.getAttribute("user_CustomerAttr1").getLogicalDataType(), LogicalDataType.Date);

        Assert.assertNotNull(template.getAttribute("user_CustomerAttr2"));
        Assert.assertEquals(template.getAttribute("user_CustomerAttr2").getLogicalDataType(), LogicalDataType.Date);
        Assert.assertEquals(template.getAttribute("user_CustomerAttr2").getDateFormatString(), "MM/DD/YYYY");
        Assert.assertTrue(StringUtils.isEmpty(template.getAttribute("user_CustomerAttr2").getTimeFormatString()));
        Assert.assertEquals(template.getAttribute("user_CustomerAttr2").getTimezone(), "UTC");

        Attribute attribute = template.getAttribute(InterfaceName.CompanyName);
        Assert.assertEquals(attribute.getDisplayName(), "CustomerName");

        Assert.assertNotNull(template.getAttribute(InterfaceName.WebVisitDate));
        Assert.assertEquals(template.getAttribute(InterfaceName.WebVisitDate).getDateFormatString(), "MM/DD/YYYY");
        Assert.assertEquals(template.getAttribute(InterfaceName.WebVisitDate).getTimeFormatString(), "00:00:00 12H");
        Assert.assertEquals(template.getAttribute(InterfaceName.WebVisitDate).getTimezone(), "UTC");

        // verify stream is created correctly
        verifyWebVisitStream(webVisitStreamTask, InterfaceName.PathPatternId.name(), false);
        verifyWebVisitStream(webVisitStreamTask, InterfaceName.SourceMediumId.name(), false);
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testCreateWebVisitTemplate" })
    public void testCreateSourceMediumTemplate() {
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

    @Test(groups = "deployment-app", dependsOnMethods = { "testCreateSourceMediumTemplate" })
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

        // verify both catalogs are attached to stream dimension
        verifyWebVisitStream(webVisitStreamTask, InterfaceName.PathPatternId.name(), true);
        verifyWebVisitStream(webVisitStreamTask, InterfaceName.SourceMediumId.name(), true);
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testCreateWebVisitPatternTemplate" })
    private void testBackupTemplateAndRestore() {
        String backupName = cdlProxy.backupTemplate(mainCustomerSpace, webVisitStreamTask.getUniqueId());
        Assert.assertFalse(StringUtils.isEmpty(backupName));

        Table restore = cdlProxy.restoreTemplate(mainCustomerSpace, webVisitStreamTask.getUniqueId(), backupName, true);
        Assert.assertNotNull(restore);

        Assert.assertEquals(restore.getAttributes().size(), webVisitStreamTask.getImportTemplate().getAttributes().size());
        Assert.assertEquals(restore.getName(), webVisitStreamTask.getImportTemplate().getName());
        for (Attribute webVisitAttr : webVisitStreamTask.getImportTemplate().getAttributes()) {
            Attribute restoreAttr = restore.getAttribute(webVisitAttr.getName());
            Assert.assertNotNull(restoreAttr);
            Assert.assertEquals(restoreAttr.getName(), webVisitAttr.getName());
            Assert.assertEquals(restoreAttr.getDisplayName(), webVisitAttr.getDisplayName());
            Assert.assertEquals(restoreAttr.getPhysicalDataType(), webVisitAttr.getPhysicalDataType());
            Assert.assertEquals(restoreAttr.getRequired(), webVisitAttr.getRequired());
        }

        Assert.expectThrows(LedpException.class, () -> cdlProxy.restoreTemplate(mainCustomerSpace, "ErrorId",
                "RandomName.json", true));
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
        boolean result = cdlProxy.createWebVisitTemplate(mainCustomerSpace, Collections.singletonList(metadata));
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

        SimpleTemplateMetadata.SimpleTemplateAttribute standardAttr2 = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        standardAttr2.setDisplayName("CustomerDate");
        standardAttr2.setName(InterfaceName.WebVisitDate.name());
        standardAttr2.setDateFormat("MM/DD/YYYY");
        standardAttr2.setTimeFormat("00:00:00 12H");
        standardAttr2.setTimeZone("UTC");

        standardList.add(standardAttr2);

        // custom attrs
        List<SimpleTemplateMetadata.SimpleTemplateAttribute> customerList = new ArrayList<>();
        SimpleTemplateMetadata.SimpleTemplateAttribute customerAttr1 = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        customerAttr1.setDisplayName("CustomerAttr1");
        customerAttr1.setName("CustomerAttr1");
        customerAttr1.setPhysicalDataType(Schema.Type.LONG);
        customerAttr1.setLogicalDataType(LogicalDataType.Date);
        customerList.add(customerAttr1);

        SimpleTemplateMetadata.SimpleTemplateAttribute customerAttr2 =
                new SimpleTemplateMetadata.SimpleTemplateAttribute();
        customerAttr2.setDisplayName("CustomerAttr2");
        customerAttr2.setName("CustomerAttr2");
        customerAttr2.setPhysicalDataType(Schema.Type.LONG);
        customerAttr2.setLogicalDataType(LogicalDataType.Date);
        customerAttr2.setDateFormat("MM/DD/YYYY");
        customerAttr2.setTimeZone("UTC");
        customerList.add(customerAttr2);

        metadata.setIgnoredStandardAttributes(new HashSet<>(ignoredStandardAttrs));
        metadata.setCustomerAttributes(customerList);
        metadata.setStandardAttributes(standardList);
        return metadata;
    }

    @Test(groups = "manual", dependsOnMethods = "testCreateWebVisitTemplate", enabled = false)
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
