package com.latticeengines.proxy.exposed.cdl;

import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessRequest;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;

public interface DropBoxProxy {

    DropBoxSummary getDropBox(String customerSpace);

    GrantDropBoxAccessResponse grantAccess(String customerSpace, GrantDropBoxAccessRequest request);

}
