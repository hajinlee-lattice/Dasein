package com.latticeengines.eai.exposed.service;

import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.domain.exposed.source.SourceCredentialType;

public interface EaiCredentialValidationService {

    void validateSourceCredential(String customerSpace, String crmType, SourceCredentialType sourceCredentialType);

    void validateSourceCredential(String customerSpace, String sourceType, CrmCredential crmCredential);

}
