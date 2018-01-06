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

        if (url.contains("+")) {
            // adding this workaround as in qa env + sigh is not getting
            // replaced due to changes in new spring library. Without + sign
            // getting replaced, SAMLResponse generate invalid XML and saml
            // library fails exception
            url = url.replaceAll("+", "%2B");
        }
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
