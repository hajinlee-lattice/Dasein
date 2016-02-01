package com.latticeengines.prospectdiscovery.workflow.steps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("registerAttributeLevelSummaryReport")
public class RegisterAttributeLevelSummaryReport extends BaseWorkflowStep<TargetMarketStepConfiguration> {

    private RegisterReport registerAttrLevelSummary = new RegisterAttributeLevelSummary();
    private RegisterReport registerAttrLevelProbSummary = new RegisterAttributeLevelProbSummary();

    public void setConfiguration(TargetMarketStepConfiguration configuration) {
        this.configuration = configuration;
    }
    
    @Override
    public boolean setup() {
        restTemplate.setInterceptors(addMagicAuthHeaders);
        return true;
    }

    @Override
    public void execute() {
    }
    
    public void execute(String name, Object[] params) {
        RegisterReport registerReport = registerAttrLevelSummary;
        if (name.endsWith("Probability")) {
            registerReport = registerAttrLevelProbSummary;
        }
        
        registerReport.setConfiguration(configuration);
        registerReport.setRestTemplate(restTemplate);
        registerReport.execute(name, ReportPurpose.getReportPurpose(name), params);
    }
    
    public RegisterReport getReportRegistrator() {
        return registerAttrLevelSummary;
    }

    static class RegisterAttributeLevelSummary extends RegisterReport {
        
        static class Stats {
            long yourCustomers = 0;
            long inYourDatabase = 0;
        }

        public ObjectNode buildJson(List<GenericRecord> records, Object[] params) {
            Map<String, Stats> stats = new HashMap<>();
            for (GenericRecord record : records) {
                Utf8 utf8 = (Utf8) record.get((String) params[0]);
                String value = null;
                
                if (utf8 != null) {
                    value = utf8.toString();
                }

                if (value == null) {
                    value = "No Value";
                }
                Boolean yourCustomer = (Boolean) record.get((String) params[1]);
                Long count = (Long) record.get("Count");
                Stats statsForValue = stats.get(value);
                
                if (statsForValue == null) {
                    statsForValue = new Stats();
                    stats.put(value, statsForValue);
                }
                statsForValue.inYourDatabase += count;
                if (yourCustomer) {
                    statsForValue.yourCustomers = count;
                }
            }
            
            ObjectNode json = new ObjectMapper().createObjectNode();
            
            ArrayNode array = json.putArray("records");
            for (Map.Entry<String, Stats> entry : stats.entrySet()) {
                ObjectNode node = array.addObject();
                node.put("value", entry.getKey());
                node.put("your_customer_count", entry.getValue().yourCustomers);
                node.put("in_your_db_count", entry.getValue().inYourDatabase);
            }
            
            return json;
        }
    }

    static class RegisterAttributeLevelProbSummary extends RegisterReport {

        public ObjectNode buildJson(List<GenericRecord> records, Object[] params) {
            ObjectNode json = new ObjectMapper().createObjectNode();
            
            ArrayNode array = json.putArray("records");
            for (GenericRecord record : records) {
                Utf8 utf8 = (Utf8) record.get((String) params[0]);
                String value = null;
                
                if (utf8 != null) {
                    value = utf8.toString();
                }

                if (value == null) {
                    value = "No Value";
                }
                Double lift = (Double) record.get("Lift");

                ObjectNode node = array.addObject();
                node.put("value", value);
                node.put("lift", lift);
            }
            
            return json;

        }
    }
}
