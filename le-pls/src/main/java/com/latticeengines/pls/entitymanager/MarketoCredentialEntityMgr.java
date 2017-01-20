package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.MarketoCredential;

public interface MarketoCredentialEntityMgr extends BaseEntityMgr<MarketoCredential> {

    MarketoCredential findMarketoCredentialById(String credentialId);

    List<MarketoCredential> findAllMarketoCredentials();

    void updateMarketoCredentialById(String credentialId, MarketoCredential marketoCredential);

    void deleteMarketoCredentialById(String credentialId);

}
