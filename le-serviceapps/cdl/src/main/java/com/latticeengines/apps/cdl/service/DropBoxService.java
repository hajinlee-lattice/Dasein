package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.cdl.DropBox;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessRequest;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.cdl.RevokeDropBoxAccessRequest;

public interface DropBoxService {

    DropBox create();

    void delete();

    String getDropBoxBucket();
    String getDropBoxPrefix();

    GrantDropBoxAccessResponse grantAccess(GrantDropBoxAccessRequest request);
    void revokeAccess(RevokeDropBoxAccessRequest request);

}
