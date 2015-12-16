package com.latticeengines.prospectdiscovery.workflow;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.prospectdiscovery.workflow.steps.RegisterImportSummaryReport;

public class RegisterImportSummaryReportUnitTestNG {
    private Configuration yarnConfiguration = new Configuration();
    private RegisterImportSummaryReport registerImportSummaryReport = new RegisterImportSummaryReport();

    @Test(groups = "unit")
    public void testGenerateJson() throws Exception {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource resource = resolver
                .getResource("com/latticeengines/prospectdiscovery/ImportSummary/ImportSummary.avro");

        List<GenericRecord> records = AvroUtils.getData(yarnConfiguration, new Path(resource.getFile().getPath()));
        assertEquals(records.size(), 1);
        GenericRecord stats = records.get(0);
        ObjectNode json = registerImportSummaryReport.buildJson(stats);
        double matchRate = json.get("accounts").get("match_rate").asDouble();
        assertTrue(matchRate >= 0.0 && matchRate <= 1.0);
    }
}
