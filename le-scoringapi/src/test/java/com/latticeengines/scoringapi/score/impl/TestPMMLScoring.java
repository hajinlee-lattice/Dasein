package com.latticeengines.scoringapi.score.impl;

import java.io.IOException;
import java.util.Date;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.scoringapi.ModelDetail;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.scoringapi.controller.InternalScoringResourceDeploymentTestNG;

@Component
public class TestPMMLScoring extends InternalScoringResourceDeploymentTestNG {
    private static final int MAX_COUNTER_FOR_PMML_SCORE = 10;

    public void scoreRecords(String modelName, CustomerSpace customerSpace, Tenant pmmlTenant)
            throws IOException, InterruptedException {
        final String url = apiHostPort + "/score/records";
        runScoringTest(url, modelName, customerSpace, pmmlTenant, true, true);
    }

    public Model getModel(String modelName, CustomerSpace customerSpace, Tenant pmmlTenant)
            throws IOException, InterruptedException {
        Thread.sleep(10000);
        for (int i = 0; i < MAX_COUNTER_FOR_PMML_SCORE; i++) {
            for (ModelDetail modelDetail : internalScoringApiProxy.getPaginatedModels(new Date(0),
                    true, 0, 50, customerSpace.toString())) {
                if (modelName.equals(modelDetail.getModel().getName())) {
                    return modelDetail.getModel();
                }
            }
            Thread.sleep(2000 * i);
        }

        return null;
    }

}
