package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.MarketoCredential;

public interface MarketoCredentialService {

    void createMarketoCredential(MarketoCredential marketoCredential);

    void deleteMarketoCredentialById(String credentialId);

    MarketoCredential findMarketoCredentialById(String credentialId);

    List<MarketoCredential> findAllMarketoCredentials();

    void updateMarketoCredentialById(String credentialId, MarketoCredential marketoCredential);

}
