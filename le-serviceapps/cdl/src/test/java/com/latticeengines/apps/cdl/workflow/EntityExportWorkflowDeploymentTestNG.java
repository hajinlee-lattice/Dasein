package com.latticeengines.apps.cdl.workflow;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.ReflectionUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.s3.model.Tag;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.cdl.end2end.CDLEnd2EndDeploymentTestNGBase;
import com.latticeengines.apps.cdl.end2end.UpdateTransactionWithAdvancedMatchDeploymentTestNG;
import com.latticeengines.apps.cdl.service.AtlasExportService;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.S3ExportFolderService;
import com.latticeengines.apps.cdl.testframework.CDLWorkflowFrameworkDeploymentTestNGBase;
import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.cdl.EntityExportRequest;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;
import com.latticeengines.domain.exposed.serviceflows.cdl.EntityExportWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.EntityExportStepConfiguration;
import com.latticeengines.domain.exposed.spark.common.ConvertToCSVConfig;

/**
 * dpltc deploy -a admin,pls,lp,cdl,metadata,matchapi,workflowapi
 */
public class EntityExportWorkflowDeploymentTestNG extends CDLWorkflowFrameworkDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(EntityExportWorkflowDeploymentTestNG.class);

    @Inject
    private AttrConfigService attrConfigService;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private EntityExportWorkflowSubmitter entityExportWorkflowSubmitter;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private AtlasExportService atlasExportService;

    @Inject
    private S3ExportFolderService s3ExportFolderService;

    @Inject
    private S3Service s3Service;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Value("${cdl.atlas.export.dropfolder.tag}")
    private String dropFolderTag;

    @Value("${cdl.atlas.export.dropfolder.tag.value}")
    private String dropFolderTagValue;

    private AtlasExport atlasExport;
    protected boolean saveCsvToLocal;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironment();
        checkpointAutoService.setMainTestTenant(mainTestTenant);
        checkpointAutoService.resumeCheckpoint( //
                UpdateTransactionWithAdvancedMatchDeploymentTestNG.CHECK_POINT, //
                CDLEnd2EndDeploymentTestNGBase.S3_CHECKPOINTS_VERSION);
        configExportAttrs();
        saveCsvToLocal = false;
    }

    @BeforeClass(groups = "manual")
    public void setupManual() throws Exception {
        boolean useExistingTenant = false;
        if (useExistingTenant) {
            testBed.useExistingTenantAsMain("LETest1558869439194");
            testBed.switchToSuperAdmin();
            mainTestTenant = testBed.getMainTestTenant();
            mainTestCustomerSpace = CustomerSpace.parse(mainTestTenant.getId());
            MultiTenantContext.setTenant(mainTestTenant);
        } else {
            setupTestEnvironment();
            checkpointService.resumeCheckpoint( //
                    "update3", //
                    CDLEnd2EndDeploymentTestNGBase.S3_CHECKPOINTS_VERSION);
            configExportAttrs();
        }
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
        saveCsvToLocal = true;
    }

    @Override
    @Test(groups = {"deployment", "manual"})
    public void testWorkflow() throws Exception {
        EntityExportRequest request = new EntityExportRequest();
        DataCollection.Version version = dataCollectionService.getActiveVersion(mainCustomerSpace);
        request.setDataCollectionVersion(version);
        atlasExport = atlasExportService.createAtlasExport(mainTestCustomerSpace.toString(),
                AtlasExportType.ACCOUNT_AND_CONTACT);
        Method method = ReflectionUtils.findMethod(EntityExportWorkflowSubmitter.class,
                "configure", String.class, EntityExportRequest.class, AtlasExport.class, Boolean.class);
        Assert.assertNotNull(method);
        ReflectionUtils.makeAccessible(method);
        EntityExportWorkflowConfiguration configuration = (EntityExportWorkflowConfiguration) //
                ReflectionUtils.invokeMethod(method, entityExportWorkflowSubmitter, //
                        mainTestCustomerSpace.toString(), request, atlasExport, true);
        Assert.assertNotNull(configuration);

        EntityExportStepConfiguration stepConfiguration = JsonUtils.deserialize( //
                configuration.getStepConfigRegistry().get(EntityExportStepConfiguration.class.getSimpleName()), //
                EntityExportStepConfiguration.class);
        stepConfiguration.setSaveToLocal(true);
        configuration.add(stepConfiguration);

        runWorkflow(generateWorkflowTestConfiguration(null, //
                "entityExportWorkflow", configuration, null));

        verifyTest();
    }

    @Override
    protected void verifyTest() {
        atlasExport = atlasExportService.getAtlasExport(mainTestCustomerSpace.toString(), atlasExport.getUuid());
        Assert.assertNotNull(atlasExport);
        Assert.assertNotNull(atlasExport.getFilesUnderDropFolder());
        Assert.assertTrue(atlasExport.getFilesUnderDropFolder().size() > 0);
        Assert.assertEquals(atlasExport.getFilesUnderDropFolder().size(), 1);
        String targetPath = s3ExportFolderService.getDropFolderExportPath(mainTestCustomerSpace.toString(),
                atlasExport.getExportType(), atlasExport.getDatePrefix(), "");
        boolean hasAccountCsv = false;
        List<String> deletePathList = atlasExport.getFilesToDelete();
        Assert.assertTrue(CollectionUtils.isNotEmpty(deletePathList));
        for (String path : deletePathList) {
            try {
                // make sure temp avro and csv files were deleted.
                Assert.assertFalse(HdfsUtils.fileExists(yarnConfiguration, path));
            } catch (IOException e) {
                log.error(String.format("Could not delete temp export path %s", e.getMessage()));
            }
        }
        for (String fileName : atlasExport.getFilesUnderDropFolder()) {

            Assert.assertTrue(s3Service.objectExist(s3Bucket, targetPath + fileName));
            List<Tag> tagList = s3Service.getObjectTags(s3Bucket, targetPath + fileName);
            Assert.assertTrue(tagList.size() > 0);
            Assert.assertEquals(tagList.get(0).getKey(), dropFolderTag);
            Assert.assertEquals(tagList.get(0).getValue(), dropFolderTagValue);
            Pattern pattern = Pattern.compile("^AccountContact.*\\.csv\\.gz$");
            Matcher matcher = pattern.matcher(fileName);
            if (matcher.find()) {
                hasAccountCsv = true;
                InputStream csvStream = s3Service.readObjectAsStream(s3Bucket, targetPath + fileName);
                if (saveCsvToLocal) {
                    try {
                        File tgtFile = new File("Account.csv.gz");
                        FileUtils.deleteQuietly(tgtFile);
                        FileUtils.copyInputStreamToFile(csvStream, tgtFile);
                    } catch (IOException e) {
                        log.warn("Failed to save csv to local.");
                    }
                    csvStream = s3Service.readObjectAsStream(s3Bucket, targetPath + fileName);
                }
                verifyAccountContactCsvGz(csvStream);
            }
        }
        Assert.assertTrue(hasAccountCsv, "Cannot find Account csv in s3 folder.");
    }

    protected void configExportAttrs() {
        AttrConfig enable1 = enableExport(BusinessEntity.Account, "user_Test_Date"); // date attr
        AttrConfig enable2 = enableExport(BusinessEntity.Account, "TechIndicator_OracleCommerce"); // bit encode
        AttrConfig enable3 = enableExport(BusinessEntity.PurchaseHistory,
                "AM_g8cH04Lzvb0Mhou2lvuuSJjjvQm1KQ3J__W_1__SW"); // activity metric
        // TODO: need to make a non-segmentable attribute enabled
        AttrConfigRequest request2 = new AttrConfigRequest();
        request2.setAttrConfigs(Arrays.asList(enable1, enable2, enable3));
        attrConfigService.saveRequest(request2, AttrConfigUpdateMode.Usage);
    }

    private AttrConfig enableExport(BusinessEntity entity, String attribute) {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setEntity(entity);
        attrConfig.setAttrName(attribute);
        AttrConfigProp<Boolean> enrichProp = new AttrConfigProp<>();
        enrichProp.setCustomValue(Boolean.TRUE);
        attrConfig.setAttrProps(new HashMap<>(ImmutableMap.of(ColumnSelection.Predefined.Enrichment.name(), enrichProp)));
        return attrConfig;
    }

    private void verifyAccountContactCsvGz(InputStream s3Stream) {
        try {
            InputStream gzipIs = new GZIPInputStream(s3Stream);
            Reader in = new InputStreamReader(gzipIs);
            CSVParser records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
            Map<String, Integer> headerMap = records.getHeaderMap();
            verifyCsvGzHeder(headerMap);
            // FIXME: This purchase history attribute is deprecated due to
            // product table doesn't exist in S3. Should improve checkpoint.
            // Assert.assertTrue(headerMap.containsKey("CMT3: Glassware: % Share
            // of Wallet in last 1 week"),
            // "Header map: " + JsonUtils.serialize(headerMap));
            for (CSVRecord record : records) {
                String dateStr = record.get("Test Date");
                if (StringUtils.isNotEmpty(dateStr)) {
                    SimpleDateFormat dateFmt = new SimpleDateFormat(ConvertToCSVConfig.ISO_8601);
                    try {
                        Date date = dateFmt.parse(dateStr);
                        Assert.assertNotNull(date);
                    } catch (ParseException e) {
                        Assert.fail("Failed to parse date string " + dateStr, e);
                    }
                    String decodedStr = record.get("Has Oracle Commerce");
                    if (StringUtils.isNotBlank(decodedStr)) {
                        Assert.assertTrue("Yes".equals(decodedStr) || "No".equals(decodedStr), //
                                "Invalid decoded value " + decodedStr);
                        break; // only need to verify the first record
                    }
                }
            }
        } catch (IOException e) {
            Assert.fail("Failed to verify account csv.", e);
        }
    }

    protected void verifyCsvGzHeder(Map<String, Integer> headerMap) {
        // make sure header map contains account and contact id
        Assert.assertTrue(headerMap.containsKey("Id"), "Header map: " + JsonUtils.serialize(headerMap));
        Assert.assertTrue(headerMap.containsKey("ContactId"), "Header map: " + JsonUtils.serialize(headerMap));
        Assert.assertTrue(headerMap.containsKey("Test Date"), "Header map: " + JsonUtils.serialize(headerMap));
        Assert.assertTrue(headerMap.containsKey("Has Oracle Commerce"), "Header map: " + JsonUtils.serialize(headerMap));
        Assert.assertTrue(headerMap.containsKey(InterfaceName.LatticeExportTime.name()), "Header map: " + JsonUtils.serialize(headerMap));
    }
}
