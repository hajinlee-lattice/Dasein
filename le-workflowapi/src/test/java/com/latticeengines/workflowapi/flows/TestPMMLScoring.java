package com.latticeengines.workflowapi.flows;

import java.io.IOException;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.scoringapi.ModelDetail;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.network.exposed.scoringapi.InternalScoringApiInterface;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.scoringapi.controller.InternalScoringResourceDeploymentTestNG;

@Component
public class TestPMMLScoring extends InternalScoringResourceDeploymentTestNG {

    private static final int MAX_COUNTER = 10;

    @Autowired
    protected InternalScoringApiInterface internalScoringApiProxy;

    public void scoreRecords(String modelName, CustomerSpace customerSpace, Tenant pmmlTenant)
            throws IOException, InterruptedException {
        final String url = apiHostPort + "/score/records";
        InternalResourceRestApiProxy plsRest = new InternalResourceRestApiProxy(plsApiHostPort);
        runScoringTest(url, plsRest, modelName, customerSpace, pmmlTenant, true, true);
    }

    public Model getMeodel(String modelName, CustomerSpace customerSpace, Tenant pmmlTenant)
            throws IOException, InterruptedException {
        Thread.sleep(10000);
        for (int i = 0; i < MAX_COUNTER; i++) {
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
