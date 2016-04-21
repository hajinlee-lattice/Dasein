package com.latticeengines.pls.end2end;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.collect.Sets;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBaseDeprecated;

@Component
public class SelfServeModelingToScoringEndToEndDeploymentTestNG extends PlsDeploymentTestNGBaseDeprecated {

    private static final Log log = LogFactory.getLog(SelfServeModelingToScoringEndToEndDeploymentTestNG.class);
    private static final String RESOURCE_BASE = "com/latticeengines/pls/end2end/selfServiceModeling/csvfiles";

    @Autowired
    private SelfServiceModelingEndToEndDeploymentTestNG selfServiceModeling;

    @Autowired
    private ScoreCorrectnessService scoreCompareService;

    private Tenant tenant;

    @BeforeClass(groups = "deployment.lp")
    public void setup() throws Exception {
        selfServiceModeling.setup();
        switchToSuperAdmin();
        tenant = selfServiceModeling.getTenant();
        log.info(tenant);
    }

    @Test(groups = "deployment.lp", enabled = true)
    public void testLeadModelToScoreCorrectness() throws InterruptedException, IOException {
        String fileName = "Mulesoft_MKTO_LP3_ScoringLead_20160316_170113.csv";
        String modelId = selfServiceModeling.prepareModel(SchemaInterpretation.SalesforceLead, unknownColumnHandler, fileName);
        scoreCompareService.analyzeScores(tenant.getId(), RESOURCE_BASE + "/" + fileName, modelId, 1000);
    }

    @Test(groups = "deployment.lp", enabled = false)
    public void testAccountModelToScoreCorrectness() throws InterruptedException, IOException {
        String fileName = "Mulesoft_SFDC_LP3_ModelingAccount_20160412_3kRows.csv";
        String modelId = selfServiceModeling.prepareModel(SchemaInterpretation.SalesforceAccount, null, fileName);
        scoreCompareService.analyzeScores(tenant.getId(), RESOURCE_BASE + "/" + fileName, modelId, 100);
    }

    Function<List<LinkedHashMap<String, String>>, Void> unknownColumnHandler = new Function<List<LinkedHashMap<String, String>>, Void>() {
        @Override
        public Void apply(List<LinkedHashMap<String, String>> unknownColumns) {
            Set<String> booleanSet = Sets.newHashSet(new String[] { "Interest_esb__c", "Interest_tcat__c",
                    "kickboxAcceptAll", "Free_Email_Address__c", "kickboxFree", "Unsubscribed", "kickboxDisposable",
                    "HasAnypointLogin", "HasCEDownload", "HasEEDownload" });
            Set<String> strSet = Sets.newHashSet(new String[] { "Lead_Source_Asset__c", "kickboxStatus", "SICCode",
                    "Source_Detail__c", "Cloud_Plan__c" });
            log.info(unknownColumns);
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
            log.info(unknownColumns);

            return null;
        }
    };

}
