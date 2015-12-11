package com.latticeengines.network.exposed.eai;

import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.CrmCredential;

public interface ValidateCredentialInterface {

    SimpleBooleanResponse validateCredential(String customerSpace, String sourceType, CrmCredential crmCredential);

}
