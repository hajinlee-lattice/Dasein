package com.latticeengines.pls.end2end;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

@Component
public class SelfServeModelingToScoringEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Autowired
    private SelfServiceModelingEndToEndDeploymentTestNG selfServiceModeling;

    @Autowired
    private ScoreCorrectnessService scoreCompareService;

    private static final String RESOURCE_BASE = "com/latticeengines/pls/end2end/selfServiceModeling/csvfiles";

    private SourceFile sourceFile;

//    private String fileName = "Mulesoft_MKTO_LP3_ModelingLead_OneLeadPerDomain_20160316_170113.csv";
    private String fileName = "Mulesoft_MKTO_LP3_ScoringLead_20160316_170113.csv";

    private Tenant tenant;

    @BeforeClass(groups = "deployment.lp")
    public void setup() throws Exception {
        selfServiceModeling.setup();
        switchToSuperAdmin();
        tenant = selfServiceModeling.getTenant();
        System.out.println(tenant);
    }

    @Test(groups = "deployment.lp", enabled = true)
    public void testEndToEnd() throws InterruptedException, IOException {
        selfServiceModeling.setFileName(fileName);
        System.out.println("Uploading File");
        selfServiceModeling.uploadFile();
        sourceFile = selfServiceModeling.getSourceFile();
        System.out.println(sourceFile.getName());
        System.out.println("Resolving Metadata");
        resolveMetadata();
        System.out.println("Creatinging Model");
        selfServiceModeling.createModel();
        selfServiceModeling.retrieveModelSummary();
        ModelSummary modelSummary = selfServiceModeling.getModelSummary();
        String modelId = modelSummary.getId();
        System.out.println("modeling id: " + modelId);
        scoreCompareService.analyzeScores(tenant.getId(), RESOURCE_BASE + "/" + fileName, modelId);
    }

    @SuppressWarnings("rawtypes")
    private void resolveMetadata() {
        RestTemplate restTemplate = selfServiceModeling.getRestTemplate();
        ResponseDocument response = restTemplate.getForObject(
                String.format("%s/pls/models/fileuploads/%s/metadata/unknown", getPLSRestAPIHostPort(),
                        sourceFile.getName()), ResponseDocument.class);
        @SuppressWarnings("unchecked")
        List<LinkedHashMap> unknownColumns = new ObjectMapper().convertValue(response.getResult(), List.class);
        setUnknowColumnType(unknownColumns);
        response = restTemplate.postForObject(
                String.format("%s/pls/models/fileuploads/%s/metadata/unknown", getPLSRestAPIHostPort(),
                        sourceFile.getName()), unknownColumns, ResponseDocument.class);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void setUnknowColumnType(List<LinkedHashMap> unknownColumns) {
        Set<String> booleanSet = Sets.newHashSet(new String[] { "Interest_esb__c", "Interest_tcat__c",
                "kickboxAcceptAll", "Free_Email_Address__c", "kickboxFree", "Unsubscribed", "kickboxDisposable",
                "HasAnypointLogin", "HasCEDownload", "HasEEDownload" });
        Set<String> strSet = Sets.newHashSet(new String[] { "Lead_Source_Asset__c", "kickboxStatus", "SICCode",
                "Source_Detail__c", "Cloud_Plan__c" });
        System.out.println(unknownColumns);
        for (LinkedHashMap<String, String> map : unknownColumns) {
            String columnName = map.get("columnName");
            if (booleanSet.contains(columnName)) {
                map.put("columnType", Schema.Type.BOOLEAN.name());
            } else if (strSet.contains(columnName)) {
                map.put("columnType", Schema.Type.STRING.name());
            } else if (columnName.startsWith("Activity_Count_")) {
                map.put("columnType", Schema.Type.INT.name());
            }
        }
        System.out.println(unknownColumns);
    }

}
