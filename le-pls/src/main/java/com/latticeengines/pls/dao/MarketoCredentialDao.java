package com.latticeengines.pls.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.MarketoCredential;

public interface MarketoCredentialDao extends BaseDao<MarketoCredential> {

    MarketoCredential findMarketoCredentialById(String credentialId);

    void deleteMarketoCredentialById(String credentialId);

}
