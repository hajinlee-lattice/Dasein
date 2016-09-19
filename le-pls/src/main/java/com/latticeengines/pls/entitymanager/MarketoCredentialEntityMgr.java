package com.latticeengines.pls.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.MarketoCredential;

import java.util.List;

public interface MarketoCredentialEntityMgr extends BaseEntityMgr<MarketoCredential> {

    MarketoCredential findMarketoCredentialById(String credentialId);

    List<MarketoCredential> findAllMarketoCredentials();

    void updateMarketoCredentialById(String credentialId, MarketoCredential marketoCredential);

    void deleteMarketoCredentialById(String credentialId);

}
