package com.latticeengines.remote.service.marketo.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.remote.marketo.LeadField;
import com.latticeengines.remote.exposed.service.marketo.MarketoSoapService;
import com.latticeengines.remote.service.impl.RemoteFunctionalTestNGBase;

public class MarketoSoapServiceTestNG extends RemoteFunctionalTestNGBase {

    private static final String SOAP_ENDPOINT = "https://548-NPP-225.mktoapi.com/soap/mktows/2_9";
    private static final String USERID = "latticedev1_51452907527AB5250FC168";
    private static final String ENCRYPTIONKEY = "5355338341004080552277AABB55226700FFCD724863";

    @Autowired
    private MarketoSoapService marketoSoapService;

    @Test(groups = "functional")
    public void testValidateMarketoSoapCredentials() {
        String soapEndPoint = SOAP_ENDPOINT;
        String userId = USERID;
        String encryptionKey = ENCRYPTIONKEY;

        boolean result = marketoSoapService.validateMarketoSoapCredentials(soapEndPoint, userId,
                encryptionKey);
        assertEquals(result, true);

        LedpException ledpException = null;

        try {
            marketoSoapService.validateMarketoSoapCredentials("malformedSoapEndpoint", userId,
                    encryptionKey);
        } catch (LedpException e) {
            ledpException = e;
        }
        assertEquals(ledpException.getCode(), LedpCode.LEDP_21034);

        try {
            marketoSoapService.validateMarketoSoapCredentials(
                    "https://invalidhost123.mktorest.com/rest", userId, encryptionKey);
        } catch (LedpException e) {
            ledpException = e;
        }
        assertEquals(ledpException.getCode(), LedpCode.LEDP_21035);

        try {
            marketoSoapService.validateMarketoSoapCredentials(soapEndPoint, "bogusUserId",
                    encryptionKey);
        } catch (LedpException e) {
            ledpException = e;
        }
        assertEquals(ledpException.getCode(), LedpCode.LEDP_21035);

        try {
            marketoSoapService.validateMarketoSoapCredentials(soapEndPoint, userId,
                    "bogusEncryptionKey");
        } catch (LedpException e) {
            ledpException = e;
        }
        assertEquals(ledpException.getCode(), LedpCode.LEDP_21035);
    }

    @Test(groups = "functional", enabled = true)
    public void testGetLeadFields() throws Exception {
        List<LeadField> fields = marketoSoapService.getLeadFields(SOAP_ENDPOINT, USERID,
                ENCRYPTIONKEY);
        assert (fields.size() > 100);
        /*
         * for (LeadField leadField : fields) {
         * assertTrue(leadField.getDataType().equals("string") ||
         * leadField.getDataType().equals("email")); }
         */
    }
}
