package com.latticeengines.remote.service.marketo.impl;

import static org.testng.Assert.assertEquals;

import javax.inject.Inject;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.remote.exposed.service.marketo.MarketoRestValidationService;
import com.latticeengines.remote.service.impl.RemoteFunctionalTestNGBase;
public class MarketoRestValidationServiceTestNG extends RemoteFunctionalTestNGBase {

    @Inject
    private MarketoRestValidationService marketoRestValidationService;

    @Test(groups = "functional", enabled = true)
    public void testValidateMarketoRestCredentials() throws Exception {
        String identityEndPoint = "https://548-NPP-225.mktorest.com/identity";
        String restEndPoint = "https://548-NPP-225.mktorest.com/rest";
        String clientId = "aecb7220-299a-44e3-8906-17a8d319c8f0";
        String clientSecret = "EFTBS3vDrMsWcTZ0w9rnQ6XYSmiWasMa";

        boolean result = marketoRestValidationService.validateMarketoRestCredentials(identityEndPoint, restEndPoint,
                clientId, clientSecret);
        assertEquals(result, true);

        LedpException ledpException = null;

        try {
            marketoRestValidationService.validateMarketoRestCredentials("malformedIdEndpoint", restEndPoint, clientId,
                    clientSecret);
        } catch (LedpException e) {
            ledpException = e;
        }
        assertEquals(ledpException.getCode(), LedpCode.LEDP_21028);

        try {
            marketoRestValidationService.validateMarketoRestCredentials("https://invalidhost123.mktorest.com/identity", restEndPoint, clientId,
                    clientSecret);
        } catch (LedpException e) {
            ledpException = e;
        }
        assertEquals(ledpException.getCode(), LedpCode.LEDP_21028);

        try {
            marketoRestValidationService.validateMarketoRestCredentials(identityEndPoint, restEndPoint, "bogusClientId",
                    clientSecret);
        } catch (LedpException e) {
            ledpException = e;
        }
        assertEquals(ledpException.getCode(), LedpCode.LEDP_21030);

        try {
            marketoRestValidationService.validateMarketoRestCredentials(identityEndPoint, restEndPoint, clientId,
                    "bogusClientSecret");
        } catch (LedpException e) {
            ledpException = e;
        }
        assertEquals(ledpException.getCode(), LedpCode.LEDP_21030);

        try {
            marketoRestValidationService.validateMarketoRestCredentials(identityEndPoint, "malformedRestEndpoint", clientId,
                    clientSecret);
        } catch (LedpException e) {
            ledpException = e;
        }
        assertEquals(ledpException.getCode(), LedpCode.LEDP_21031);

        try {
            marketoRestValidationService.validateMarketoRestCredentials(identityEndPoint, "https://invalidhost123.mktorest.com/rest", clientId,
                    clientSecret);
        } catch (LedpException e) {
            ledpException = e;
        }
        assertEquals(ledpException.getCode(), LedpCode.LEDP_21031);

    }

}
