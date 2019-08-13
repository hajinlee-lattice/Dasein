package com.latticeengines.cdl.workflow.steps.validations.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public class InputFileValidationServiceUnitTestNG {

    @Test(groups = "unit")
    public void testRecord() throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream is = classLoader.getResourceAsStream("inputFileValidation/Account1.avro");
        List<GenericRecord> records = AvroUtils.readFromInputStream(is);
        Assert.assertNotNull(records);
        Assert.assertTrue(records.size() > 0);
        GenericRecord record = records.get(0);
        String accountDisplayName = InputFileValidationService.getFieldDisplayName(record,
                InterfaceName.AccountId.name(), "DefaultName");
        Assert.assertEquals(accountDisplayName, "DefaultName");
        String cityDisplayName = InputFileValidationService.getFieldDisplayName(record,
                InterfaceName.City.name(), "WrongCity");
        Assert.assertEquals(cityDisplayName, "City");
    }
}
