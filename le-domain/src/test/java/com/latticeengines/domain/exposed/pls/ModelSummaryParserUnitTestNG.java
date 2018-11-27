package com.latticeengines.domain.exposed.pls;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.Precision;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ModelSummaryParserUnitTestNG {
    private static final String RESOURCE_ROOT = "com/latticeengines/domain/exposed/pls/";

    @SuppressWarnings("deprecation")
    @Test(groups = "unit")
    public void testParse() throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(RESOURCE_ROOT + "modelsummary.json");
        StringWriter writer = new StringWriter();
        IOUtils.copy(inputStream, writer);
        String modelSummaryJsonContent = writer.toString();
        Assert.assertTrue(StringUtils.isNoneBlank(modelSummaryJsonContent));
        ModelSummaryParser modelSummaryParser = new ModelSummaryParser();
        ModelSummary modelSummary = modelSummaryParser.parse(UUID.randomUUID().toString(), modelSummaryJsonContent);
        Assert.assertNotNull(modelSummary);
        Assert.assertNotNull(modelSummary.getTop10PercentLift());
        Assert.assertNotNull(modelSummary.getTop20PercentLift());
        Assert.assertNotNull(modelSummary.getTop30PercentLift());
        Assert.assertTrue(Precision.equals(modelSummary.getTop10PercentLift(), 3.630, 0.001),
                modelSummary.getTop10PercentLift().toString());
        Assert.assertTrue(Precision.equals(modelSummary.getTop20PercentLift(), 2.597, 0.001),
                modelSummary.getTop20PercentLift().toString());
        Assert.assertTrue(Precision.equals(modelSummary.getTop30PercentLift(), 2.068, 0.001),
                modelSummary.getTop30PercentLift().toString());
    }
}
