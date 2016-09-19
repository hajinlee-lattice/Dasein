package com.latticeengines.pls.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.MarketoCredential;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;
import com.latticeengines.pls.entitymanager.MarketoCredentialEntityMgr;
import com.latticeengines.pls.entitymanager.MarketoMatchFieldEntityMgr;
import com.latticeengines.pls.service.MarketoCredentialService;

@Component("marketoCredentialService")
public class MarketoCredentialServiceImpl implements MarketoCredentialService {

    @Autowired
    private MarketoCredentialEntityMgr marketoCredentialEntityMgr;

    @Autowired
    private MarketoMatchFieldEntityMgr marketoMatchFieldEntityMgr;

    @Override
    public void createMarketoCredential(MarketoCredential marketoCredential) {
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
        marketoCredentialEntityMgr.updateMarketoCredentialById(credentialId, marketoCredential);
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

}
