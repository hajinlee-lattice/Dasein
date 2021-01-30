package com.latticeengines.pls.end2end.fileimport;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.validator.AttributeLengthValidator;
import com.latticeengines.domain.exposed.metadata.datafeed.validator.SimpleValueFilter;
import com.latticeengines.domain.exposed.metadata.datafeed.validator.TemplateValidator;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.workflow.Report;

public class CSVFileImportTemplateValidatorDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {

    protected static final String ACCOUNT_VALIDATOR_SOURCE_FILE = "Account_validator.csv";

    @BeforeClass(groups = "deployment.import.group2")
    public void setup() throws Exception {
        String featureFlag = LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName();
        Map<String, Boolean> flags = new HashMap<>();
        flags.put(featureFlag, true);
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG, flags);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        createDefaultImportSystem();
    }

    @Test(groups = "deployment.import.group2")
    public void testTemplateValidator() {
        // import file
        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_VALIDATOR_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_VALIDATOR_SOURCE_FILE));
        String feedType = getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_ACCOUNT);
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sourceFile.getName(), ENTITY_ACCOUNT, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("ID")) {
                fieldMapping.setIdType(FieldMapping.IdType.Account);
                fieldMapping.setSystemName(DEFAULT_SYSTEM);
                fieldMapping.setMapToLatticeId(true);
                fieldMapping.setRequired(true);
                fieldMapping.setLength(18);
            }
            // remove id field.
            if (InterfaceName.CustomerAccountId.name().equals(fieldMapping.getMappedField())) {
                fieldMapping.setMappedField(null);
            }
        }
        modelingFileMetadataService.resolveMetadata(sourceFile.getName(), fieldMappingDocument, ENTITY_ACCOUNT, SOURCE,
                feedType);
        sourceFile = sourceFileService.findByName(sourceFile.getName());
        // check if length attribute is stored in sourcefileconfig
        S3ImportSystem importSystem = cdlProxy.getS3ImportSystem(customerSpace, DEFAULT_SYSTEM);
        Assert.assertNotNull(importSystem);
        Assert.assertTrue(StringUtils.isNotBlank(importSystem.getAccountSystemId()));
        Assert.assertNotNull(sourceFile.getSourcefileConfig());
        Assert.assertEquals(sourceFile.getSourcefileConfig().getUniqueIdentifierConfig().getName(),
                importSystem.getAccountSystemId());
        Assert.assertEquals(sourceFile.getSourcefileConfig().getUniqueIdentifierConfig().getLength().intValue(), 18);
        Assert.assertEquals(sourceFile.getSourcefileConfig().getUniqueIdentifierConfig().isRequired(), true);

        String dataFeedTaskId = cdlService.createS3Template(customerSpace, sourceFile.getName(), SOURCE, ENTITY_ACCOUNT,
                feedType, null, ENTITY_ACCOUNT + "Data");
        Assert.assertNotNull(sourceFile);
        Assert.assertNotNull(dataFeedTaskId);

        // check if length attribute is stored in datafeedtask
        Assert.assertTrue(assertAttributeInDataFeedTask(dataFeedTaskId, importSystem.getAccountSystemId(), 18, false));

        startCDLImport(sourceFile, ENTITY_ACCOUNT, DEFAULT_SYSTEM);

        List<?> list = restTemplate.getForObject(getRestAPIHostPort() + "/pls/reports", List.class);
        List<Report> reports = JsonUtils.convertList(list, Report.class);
        Assert.assertNotNull(reports);

        Assert.assertEquals(CollectionUtils.size(reports), 1);
        verifyReport(reports.get(0), 3L, 3L, 20L);

        SimpleValueFilter simpleValueFilter = new SimpleValueFilter();
        simpleValueFilter.setReverse(true);
        SimpleValueFilter.Restriction r1 = new SimpleValueFilter.Restriction();
        r1.setFieldName("State");
        r1.setValue("PA");
        r1.setOperator(SimpleValueFilter.Restriction.Operator.EQUAL);
        SimpleValueFilter.Restriction r2 = new SimpleValueFilter.Restriction();
        r2.setFieldName("City");
        r2.setValue("BATH");
        r2.setOperator(SimpleValueFilter.Restriction.Operator.NOT_EQUAL);
        SimpleValueFilter.Restriction r3 = new SimpleValueFilter.Restriction();
        r3.setFieldName("CustomerAccountId");
        r3.setValue("00124000[0|1|2]1DNcl1AAD");
        r3.setOperator(SimpleValueFilter.Restriction.Operator.MATCH);

        simpleValueFilter.setRestrictions(Arrays.asList(r1, r2, r3));
        cdlProxy.addSimpleValueFilter(customerSpace, dataFeedTaskId, simpleValueFilter);

        startCDLImport(sourceFile, ENTITY_ACCOUNT, DEFAULT_SYSTEM);

        list = restTemplate.getForObject(getRestAPIHostPort() + "/pls/reports", List.class);
        reports = JsonUtils.convertList(list, Report.class);
        Assert.assertNotNull(reports);

        Assert.assertEquals(CollectionUtils.size(reports), 2);

        reports.sort(Comparator.comparing(Report::getCreated));
        verifyReport(reports.get(1), 6L, 6L, 17L);
    }

    public boolean assertAttributeInDataFeedTask(String dataFeedTaskId, String name, Integer length,
            boolean isNullable) {
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, dataFeedTaskId);
        Assert.assertNotNull(dataFeedTask);
        Assert.assertNotNull(dataFeedTask.getDataFeedTaskConfig());
        for (TemplateValidator validator : dataFeedTask.getDataFeedTaskConfig().getTemplateValidators()) {
            if (validator instanceof AttributeLengthValidator) {
                AttributeLengthValidator lengthValidator = (AttributeLengthValidator) validator;
                if (lengthValidator.getAttributeName().equalsIgnoreCase(name)) {
                    Assert.assertEquals(length.intValue(), lengthValidator.getLength().intValue());
                    Assert.assertTrue(isNullable == lengthValidator.getNullable());
                    return true;
                }
            }
        }
        return false;
    }
}
