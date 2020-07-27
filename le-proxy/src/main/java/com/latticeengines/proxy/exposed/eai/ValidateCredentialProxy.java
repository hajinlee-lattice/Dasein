package com.latticeengines.proxy.exposed.eai;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component
public class ValidateCredentialProxy extends MicroserviceRestApiProxy {

    public ValidateCredentialProxy() {
        super("eai/validatecredential");
    }

    public SimpleBooleanResponse validateCredential(String customerSpace, String sourceType, CrmCredential crmCredential) {
        String url = constructUrl("/customerspaces/{customerSpace}/sourcetypes/{sourceType}", customerSpace, sourceType);
        return post("validateCredential", url, crmCredential, SimpleBooleanResponse.class);
    }
}
