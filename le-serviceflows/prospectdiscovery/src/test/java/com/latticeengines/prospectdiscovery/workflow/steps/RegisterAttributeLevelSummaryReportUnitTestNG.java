package com.latticeengines.prospectdiscovery.workflow.steps;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.AvroUtils;

public class RegisterAttributeLevelSummaryReportUnitTestNG {
    
    private Configuration yarnConfiguration = new Configuration();
    
    @Test(groups = "unit", dataProvider = "attributeLevelSummary")
    public void registerAttributeLevelSummary(List<GenericRecord> records, Object[] params, int numEntries) {
        RegisterAttributeLevelSummaryReport.RegisterAttributeLevelSummary r = new RegisterAttributeLevelSummaryReport.RegisterAttributeLevelSummary();
        ObjectNode json = r.buildJson(records, params);
        ArrayNode array = (ArrayNode) json.get("records");
        assertEquals(array.size(), numEntries);
    }
    
    @Test(groups = "unit", dataProvider = "attributeLevelProbSummary")
    public void registerAttributeLevelProbSummary(List<GenericRecord> records, Object[] params, int numEntries) {
        RegisterAttributeLevelSummaryReport.RegisterAttributeLevelProbSummary r = new RegisterAttributeLevelSummaryReport.RegisterAttributeLevelProbSummary();
        ObjectNode json = r.buildJson(records, params);
        ArrayNode array = (ArrayNode) json.get("records");
        assertEquals(array.size(), numEntries);
        System.out.println(json);
    }
    
    @DataProvider(name = "attributeLevelSummary")
    public Object[][] attributeLevelSummary() throws Exception {
        return new Object[][] {
                { //
                    getRecords("com/latticeengines/prospectdiscovery/AttributeLevelSummary/emp-cnt.avro"), //
                    new Object[] { "BusinessEmployeesRange", "Event_OpportunityCreated" }, //
                    9 //
                }, //
                { //
                    getRecords("com/latticeengines/prospectdiscovery/AttributeLevelSummary/industry-cnt.avro"), //
                    new Object[] { "BusinessIndustry", "Event_OpportunityCreated" }, //
                    40 //
                }, //
                { //
                    getRecords("com/latticeengines/prospectdiscovery/AttributeLevelSummary/revenue-cnt.avro"), //
                    new Object[] { "BusinessRevenueRange", "Event_OpportunityCreated" }, //
                    7
                }
        };
    }
        
    @DataProvider(name = "attributeLevelProbSummary")
    public Object[][] attributeLevelProbSummary() throws Exception {
        return new Object[][] {
                { //
                    getRecords("com/latticeengines/prospectdiscovery/AttributeLevelProbabilitySummary/emp.avro"), //
                    new Object[] { "BusinessEmployeesRange", 0.01 }, //
                    8 //
                }, //
                { //
                    getRecords("com/latticeengines/prospectdiscovery/AttributeLevelProbabilitySummary/industry.avro"), //
                    new Object[] { "BusinessIndustry", 0.01 }, //
                    38 //
                }, //
                { //
                    getRecords("com/latticeengines/prospectdiscovery/AttributeLevelProbabilitySummary/revenue.avro"), //
                    new Object[] { "BusinessRevenueRange", 0.01 }, //
                    6
                }
        };
    }
        
    private List<GenericRecord> getRecords(String path) throws Exception {
        yarnConfiguration.set("fs.defaultFS", "file:///");
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource resource = resolver.getResource(path);
        return AvroUtils.getData(yarnConfiguration, new Path(resource.getFile().getPath()));
    }
    
}
