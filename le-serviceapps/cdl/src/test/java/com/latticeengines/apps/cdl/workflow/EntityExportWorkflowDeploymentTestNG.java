package com.latticeengines.apps.cdl.workflow;

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
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import javax.inject.Inject;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.ReflectionUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.s3.model.Tag;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.cdl.end2end.CDLEnd2EndDeploymentTestNGBase;
import com.latticeengines.apps.cdl.service.AtlasExportService;
import com.latticeengines.apps.cdl.service.S3ExportFolderService;
import com.latticeengines.apps.cdl.testframework.CDLWorkflowFrameworkDeploymentTestNGBase;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.cdl.EntityExportRequest;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;
import com.latticeengines.domain.exposed.serviceflows.cdl.EntityExportWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.EntityExportStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

/**
 * dpltc deploy -a admin,pls,lp,cdl,metadata,matchapi,workflowapi
 */
public class EntityExportWorkflowDeploymentTestNG extends CDLWorkflowFrameworkDeploymentTestNGBase {

    @Inject
    private CDLAttrConfigProxy cdlAttrConfigProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private EntityExportWorkflowSubmitter entityExportWorkflowSubmitter;

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

    @BeforeClass(groups = "manual" )
    public void setup() throws Exception {
        boolean useExistingTenant = false;
        if (useExistingTenant) {
            testBed.useExistingTenantAsMain("LETest1557310393505");
            testBed.switchToSuperAdmin();
            mainTestTenant = testBed.getMainTestTenant();
            mainTestCustomerSpace = CustomerSpace.parse(mainTestTenant.getId());
        } else {
            setupTestEnvironment();
            checkpointService.resumeCheckpoint( //
                    "update3", //
                    CDLEnd2EndDeploymentTestNGBase.S3_CHECKPOINTS_VERSION);
            configExportAttrs();
        }
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
    }

    @Test(groups = "manual")
    public void testWorkflow() throws Exception {
        DataCollection.Version version = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
        EntityExportRequest request = new EntityExportRequest();
        request.setDataCollectionVersion(version);
        atlasExport = atlasExportService.createAtlasExport(mainTestCustomerSpace.toString(),
                AtlasExportType.ACCOUNT_AND_CONTACT);
        Method method = ReflectionUtils.findMethod(EntityExportWorkflowSubmitter.class,
                "configure", String.class, EntityExportRequest.class, AtlasExport.class);
        Assert.assertNotNull(method);
        ReflectionUtils.makeAccessible(method);
        EntityExportWorkflowConfiguration configuration = (EntityExportWorkflowConfiguration) //
                ReflectionUtils.invokeMethod(method, entityExportWorkflowSubmitter, //
                        mainTestCustomerSpace.toString(), request, atlasExport);
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
        String targetPath = s3ExportFolderService.getDropFolderExportPath(mainTestCustomerSpace.toString(),
                atlasExport.getExportType(), atlasExport.getDatePrefix(), "");
        boolean hasAccountCsv = false;
        for (String fileName : atlasExport.getFilesUnderDropFolder()) {
            Assert.assertTrue(s3Service.objectExist(s3Bucket, targetPath + fileName));
            List<Tag> tagList = s3Service.getObjectTags(s3Bucket, targetPath + fileName);
            Assert.assertTrue(tagList.size() > 0);
            Assert.assertEquals(tagList.get(0).getKey(), dropFolderTag);
            Assert.assertEquals(tagList.get(0).getValue(), dropFolderTagValue);
            if (fileName.equalsIgnoreCase("Account.csv.gz")) {
                hasAccountCsv = true;
                InputStream csvStream = s3Service.readObjectAsStream(s3Bucket, targetPath + fileName);
                verifyAccountCsvGz(csvStream);
            }
        }
        Assert.assertTrue(hasAccountCsv, "Cannot find Account csv in s3 folder.");
    }

    private void configExportAttrs() {
        AttrConfig config1 = enableExport(BusinessEntity.Account, "user_Test_Date");
        AttrConfig config2 = enableExport(BusinessEntity.Account, "TechIndicator_OracleCommerce");
        AttrConfig config3 = enableExport(BusinessEntity.PurchaseHistory,
                "AM_g8cH04Lzvb0Mhou2lvuuSJjjvQm1KQ3J__W_1__SW");
        AttrConfigRequest request = new AttrConfigRequest();
        request.setAttrConfigs(Arrays.asList(config1, config2, config3));
        cdlAttrConfigProxy.saveAttrConfig(mainTestTenant.getId(), request, AttrConfigUpdateMode.Usage);
    }

    private AttrConfig enableExport(BusinessEntity entity, String attribute) {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setEntity(entity);
        attrConfig.setAttrName(attribute);
        AttrConfigProp<Boolean> enrichProp = new AttrConfigProp<>();
        enrichProp.setCustomValue(Boolean.TRUE);
        attrConfig.setAttrProps(ImmutableMap.of(ColumnSelection.Predefined.Enrichment.name(), enrichProp));
        return attrConfig;
    }

    private void verifyAccountCsvGz(InputStream s3Stream) {
        try {
            InputStream gzipIs = new GZIPInputStream(s3Stream);
            Reader in = new InputStreamReader(gzipIs);
            CSVParser records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
            Map<String, Integer> headerMap = records.getHeaderMap();
            Assert.assertTrue(headerMap.containsKey("Test Date"));
            Assert.assertTrue(headerMap.containsKey("Has Oracle Commerce"));
            Assert.assertTrue(headerMap.containsKey("CMT3: Glassware: % Share of Wallet in last 1 week"));
            for (CSVRecord record : records) {
                String dateStr = record.get("Test Date");
                SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
                try {
                    Date date = dateFmt.parse(dateStr);
                    Assert.assertNotNull(date);
                } catch (ParseException e) {
                    Assert.fail("Failed to parse date string " + dateStr, e);
                }
                String decodedStr = record.get("Has Oracle Commerce");
                if (StringUtils.isNotBlank(decodedStr)) {
                    Assert.assertTrue(decodedStr.equals("Yes") || decodedStr.equals("No"), //
                            "Invalid decoded value " + decodedStr);
                    break; // only need to verify the first record
                }
            }
        } catch (IOException e) {
            Assert.fail("Failed to verify account csv.", e);
        }
    }

}
