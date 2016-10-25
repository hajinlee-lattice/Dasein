package com.latticeengines.pls.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.MarketoCredential;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;
import com.latticeengines.pls.entitymanager.MarketoCredentialEntityMgr;
import com.latticeengines.pls.entitymanager.MarketoMatchFieldEntityMgr;
import com.latticeengines.pls.service.MarketoCredentialService;
import com.latticeengines.remote.exposed.service.marketo.MarketoRestValidationService;
import com.latticeengines.remote.exposed.service.marketo.MarketoSoapService;

@Component("marketoCredentialService")
public class MarketoCredentialServiceImpl implements MarketoCredentialService {

    @Autowired
    private MarketoCredentialEntityMgr marketoCredentialEntityMgr;

    @Autowired
    private MarketoMatchFieldEntityMgr marketoMatchFieldEntityMgr;

    @Autowired
    private MarketoRestValidationService marketoRestValidationService;

    @Autowired
    private MarketoSoapService marketoSoapService;

    @Override
    public void createMarketoCredential(MarketoCredential marketoCredential) {
        validateRESTAndSOAPCredentials(marketoCredential);
        marketoCredentialEntityMgr.create(marketoCredential);
    }

    @Override
    public void deleteMarketoCredentialById(String credentialId) {
        marketoCredentialEntityMgr.deleteMarketoCredentialById(credentialId);
    }

    @Override
    public MarketoCredential findMarketoCredentialById(String credentialId) {
        return marketoCredentialEntityMgr.findMarketoCredentialById(credentialId);
    }

    @Override
    public List<MarketoCredential> findAllMarketoCredentials() {
        return marketoCredentialEntityMgr.findAllMarketoCredentials();
    }

    @Override
    public void updateMarketoCredentialById(String credentialId,
            MarketoCredential marketoCredential) {
        validateRESTAndSOAPCredentials(marketoCredential);
        try {
            marketoCredentialEntityMgr.updateMarketoCredentialById(credentialId, marketoCredential);
        } catch (DataIntegrityViolationException e) {
            throw new LedpException(LedpCode.LEDP_18119,
                    new String[] { marketoCredential.getName() });
        }
    }

    @Override
    public void updateCredentialMatchFields(String credentialId,
            List<MarketoMatchField> marketoMatchFields) {
        MarketoCredential marketoCredential = findMarketoCredentialById(credentialId);

        for (MarketoMatchField marketoMatchField : marketoMatchFields) {
            marketoMatchFieldEntityMgr.updateMarketoMatchFieldValue(
                    marketoMatchField.getMarketoMatchFieldName(),
                    marketoMatchField.getMarketoFieldName(), marketoCredential.getEnrichment());
        }

    }

    private void validateRESTAndSOAPCredentials(MarketoCredential marketoCredential) {
        boolean restValidationResult, soapValidationResult = false;
        try {
            restValidationResult = marketoRestValidationService.validateMarketoRestCredentials(
                    marketoCredential.getRestIdentityEnpoint(), marketoCredential.getRestEndpoint(),
                    marketoCredential.getRestClientId(), marketoCredential.getRestClientSecret());
        } catch (LedpException e) {
            throw new LedpException(LedpCode.LEDP_18116, new String[] { e.getMessage() });
        }
        try {
            soapValidationResult = marketoSoapService.validateMarketoSoapCredentials(
                    marketoCredential.getSoapEndpoint(), marketoCredential.getSoapUserId(),
                    marketoCredential.getSoapEncryptionKey());
        } catch (LedpException e) {
            throw new LedpException(LedpCode.LEDP_18117, new String[] { e.getMessage() });
        }
        if (!restValidationResult) {
            throw new LedpException(LedpCode.LEDP_18116, new String[] { "bad REST credentials" });
        }
        if (!soapValidationResult) {
            throw new LedpException(LedpCode.LEDP_18117, new String[] { "bad SOAP credentials" });
        }
    }

}
