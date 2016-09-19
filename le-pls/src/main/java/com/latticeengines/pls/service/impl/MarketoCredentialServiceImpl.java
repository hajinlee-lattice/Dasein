package com.latticeengines.pls.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.MarketoCredential;
import com.latticeengines.pls.entitymanager.MarketoCredentialEntityMgr;
import com.latticeengines.pls.service.MarketoCredentialService;

@Component("marketoCredentialService")
public class MarketoCredentialServiceImpl implements MarketoCredentialService {

    @Autowired
    private MarketoCredentialEntityMgr marketoCredentialEntityMgr;

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
        return marketoCredentialEntityMgr.findAll();
    }

    @Override
    public void updateMarketoCredentialById(String credentialId,
            MarketoCredential marketoCredential) {
        marketoCredentialEntityMgr.updateMarketoCredentialById(credentialId, marketoCredential);
    }

}
