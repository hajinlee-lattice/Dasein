package com.latticeengines.proxy.exposed.saml;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.saml.LoginValidationResponse;
import com.latticeengines.domain.exposed.saml.LogoutValidationResponse;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("spSamlProxy")
public class SPSamlProxy extends BaseRestApiProxy {

    public SPSamlProxy() {
        super(PropertyUtils.getProperty("common.saml.url"), "/saml");
    }

    public LoginValidationResponse validateSSOLogin(String tenantId, Object samlSSOResponse, String relayState) {
        String url = constructUrl("/SSO/alias/{tenant}?SAMLResponse={samlSSOResponse}&RelayState={relayState}",
                tenantId, samlSSOResponse, relayState);
        return postForUrlEncoded("validateSSOLogin", url, LoginValidationResponse.class);
    }

    public LogoutValidationResponse validateSingleLogout(String tenantId, boolean isSPInitiatedLogout,
            Object samlLogoutRequest) {
        String url = constructUrl("/SingleLogout/alias/{tenant}", tenantId);
        LogoutValidationResponse resp = new LogoutValidationResponse();
        resp.setValidated(true);
        return resp;
    }

    public String getSPMetadata(String tenantId) {
        String url = constructUrl("/metadata/alias/{tenant}", tenantId);
        return get("getSPMetadata", url, String.class);
    }
}
