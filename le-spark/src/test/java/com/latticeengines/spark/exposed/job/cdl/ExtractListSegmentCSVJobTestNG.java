package com.latticeengines.spark.exposed.job.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.template.CSVAdaptor;
import com.latticeengines.domain.exposed.metadata.template.ImportFieldMapping;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ExtractListSegmentCSVConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;


public class ExtractListSegmentCSVJobTestNG extends SparkJobFunctionalTestNGBase {

    private void uploadData() {
        String[] headers = Arrays.asList("D-U-N-S® Number", "Country/Region Name", "Company Name").toArray(new String[]{});
        Object[][] fields = new Object[][]{ //
                {"1234", "USA", "D&B"}
        };
        uploadHdfsDataUnitWithCSVFmt(headers, fields);
    }

    private ImportFieldMapping getFieldMapping(String fieldName, String userFieldName, UserDefinedType fieldType) {
        ImportFieldMapping importFieldMapping = new ImportFieldMapping();
        importFieldMapping.setFieldType(fieldType);
        importFieldMapping.setUserFieldName(userFieldName);
        importFieldMapping.setFieldName(fieldName);
        return importFieldMapping;
    }

    private CSVAdaptor getCSVAdaptor() {
        CSVAdaptor csvAdaptor = new CSVAdaptor();
        List<ImportFieldMapping> importFieldMappings = new ArrayList<>();
        importFieldMappings.add(getFieldMapping("DUNS", "D-U-N-S® Number", UserDefinedType.TEXT));
        importFieldMappings.add(getFieldMapping("Country", "Country/Region Name", UserDefinedType.TEXT));
        importFieldMappings.add(getFieldMapping("CompanyName", "Company Name", UserDefinedType.TEXT));
        csvAdaptor.setImportFieldMappings(importFieldMappings);
        return csvAdaptor;
    }

    @Test(groups = "functional")
    public void test() {
        uploadData();
        ExtractListSegmentCSVConfig config = new ExtractListSegmentCSVConfig();
        config.setCsvAdaptor(getCSVAdaptor());
        config.setTargetNums(1);
        List<String> accountAttributes =
                config.getCsvAdaptor().getImportFieldMappings().stream().map(ImportFieldMapping::getFieldName).collect(Collectors.toList());
        config.setAccountAttributes(accountAttributes);
        SparkJobResult result = runSparkJob(ExtractListSegmentCSVJob.class, config);
        verifyResult(result);
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            Assert.assertEquals(record.getSchema().getFields().size(), 3);
            Assert.assertNotNull(record.getSchema().getField("DUNS"), "1234");
            Assert.assertNotNull(record.getSchema().getField("Country"), "USA");
            Assert.assertNotNull(record.getSchema().getField("CompanyName"), "D&B");
        });
        return true;
    }

}
