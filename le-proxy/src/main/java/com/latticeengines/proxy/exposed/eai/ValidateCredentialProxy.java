package com.latticeengines.proxy.exposed.eai;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.network.exposed.eai.ValidateCredentialInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component
public class ValidateCredentialProxy extends BaseRestApiProxy implements ValidateCredentialInterface {

    public ValidateCredentialProxy() {
        super("eai/validatecredential");
    }

    @Override
    public SimpleBooleanResponse validateCredential(String customerSpace, String sourceType, CrmCredential crmCredential) {
        String url = constructUrl("/customerspaces/{customerSpace}/sourcetypes/{sourceType}", customerSpace, sourceType);
        return post("validateCredential", url, crmCredential, SimpleBooleanResponse.class);
    }
}
