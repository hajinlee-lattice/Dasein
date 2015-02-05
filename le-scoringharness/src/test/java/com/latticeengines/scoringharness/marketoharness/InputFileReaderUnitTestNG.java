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

    @Test(groups = "unit")
    public void testSortInputFile() {
        try {
            URL fileResourceURL = getClass().getClassLoader().getResource("valid-input-file-reversed.json");
            InputFileReader reader = new InputFileReader(Paths.get(fileResourceURL.toURI()).toString());
            List<OperationSpec> specs = reader.read();
            long previousOffset = -1;
            for (OperationSpec spec : specs) {
                Assert.assertTrue(previousOffset <= spec.offsetMilliseconds);
                previousOffset = spec.offsetMilliseconds;
            }
        } catch (Exception e) {
            Assert.fail("Unexpected exception thrown: " + e);
        }
    }
}