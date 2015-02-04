package com.latticeengines.scoringharness.marketoharness;

import java.net.URL;
import java.nio.file.Paths;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.scoringharness.InputFileReader;
import com.latticeengines.scoringharness.operationmodel.OperationSpec;

public class InputFileReaderUnitTestNG {
    @Test(groups = "unit")
    public void testReadValidFile() {
        try {
            URL fileResourceURL = getClass().getClassLoader().getResource("valid-input-file.json");
            InputFileReader reader = new InputFileReader(Paths.get(fileResourceURL.toURI()).toString());
            @SuppressWarnings("unused")
            List<OperationSpec> specs = reader.read();
        } catch (Exception e) {
            Assert.fail("Unexpected exception thrown: " + e);
        }
    }
}