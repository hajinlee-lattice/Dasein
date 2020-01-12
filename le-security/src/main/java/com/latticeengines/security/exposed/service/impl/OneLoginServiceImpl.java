package com.latticeengines.security.exposed.service.impl;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.auth.OneLoginExternalSession;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.service.OneLoginService;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.util.OneLoginSAMLSettingUtils;
import com.latticeengines.security.util.SAMLParsingUtils;
import com.onelogin.saml2.Auth;
import com.onelogin.saml2.authn.AuthnRequest;
import com.onelogin.saml2.exception.SettingsException;
import com.onelogin.saml2.http.HttpRequest;
import com.onelogin.saml2.logout.LogoutRequest;
import com.onelogin.saml2.logout.LogoutResponse;
import com.onelogin.saml2.servlet.ServletUtils;
import com.onelogin.saml2.settings.Saml2Settings;
import com.onelogin.saml2.util.Constants;
import com.onelogin.saml2.util.Util;

@Service("oneLoginService")
public class OneLoginServiceImpl implements OneLoginService {

    private static final Logger log = LoggerFactory.getLogger(OneLoginServiceImpl.class);

    @Inject
    private SessionService sessionService;

    @Value("${security.onelogin.saml2.profile}")
    private String profile;

    private String metadata;

    @Override
    public String getSPMetadata() {
        if (this.metadata == null) {
            synchronized (this) {
                if (this.metadata == null) {
                    try {
                        Saml2Settings settings = OneLoginSAMLSettingUtils.buildSettings(profile);
                        settings.setSPValidationOnly(true);
                        String metadata = settings.getSPMetadata();
                        List<String> errors = Saml2Settings.validateMetadata(metadata);
                        if (errors.isEmpty()) {
                            this.metadata = metadata;
                        } else {
                            throw new RuntimeException("Metadata validation error: " + errors);
                        }
                    } catch (Exception e) {
                        log.warn("Failed to get SSO SP metadata.", e);
                    }
                }
            }
        }
        return this.metadata;
    }

    @Override
    public String processACS(String profile, HttpServletRequest request, HttpServletResponse response) {
        String relayState = request.getParameter("RelayState");
        String baseUrl = StringUtils.isNotBlank(relayState) ? relayState : OneLoginSAMLSettingUtils.getLoginUrl(profile);
        String redirectUrl;

        Auth auth;
        try {
            Document samlResponseDoc = SAMLParsingUtils.parseSAMLResponse(request.getParameter("SAMLResponse"));
            String issuer = SAMLParsingUtils.extractIssuer(samlResponseDoc);

            Saml2Settings settings = OneLoginSAMLSettingUtils.buildSettings(profile);
            auth = new Auth(settings, request, response);
            auth.processResponse();

            if (!auth.isAuthenticated()) {
                throw new RuntimeException("Failed to authenticate the SAML response.");
            }

            List<String> errors = auth.getErrors();
            if (CollectionUtils.isNotEmpty(errors)) {
                throw new RuntimeException("Encountered errors during SAML validation: " + errors);
            }

            OneLoginExternalSession externalSession = new OneLoginExternalSession();
            externalSession.setIssuer(issuer);
            externalSession.setNameId(auth.getNameId());
            externalSession.setSessionIndex(auth.getSessionIndex());
            externalSession.setAttributes(auth.getAttributes());
            externalSession.setProfile(profile);
            externalSession.setNameIdFormat(auth.getNameIdFormat());
            externalSession.setNameIdNameQualifier(auth.getNameIdNameQualifier());
            externalSession.setNameIdSPNameQualifier(auth.getNameIdSPNameQualifier());
            externalSession.setExpiration(auth.getSessionExpiration().getMillis());
            log.info("External OneLogin Session: " + JsonUtils.serialize(externalSession));

            String userEmail = auth.getNameId();
            Ticket ticket = sessionService.authenticateSamlUser(userEmail, externalSession);
            log.info("Generated GA token: " + ticket.getData());
            String encodedToken = URLEncoder.encode(ticket.getData(), "UTF-8");
            redirectUrl = String.format("%s/login/?token=%s", baseUrl, encodedToken);
        } catch (Exception e) {
            log.warn("Failed to validate OneLogin SAML response.", e);
            redirectUrl = String.format("%s/login/error", baseUrl);
        }

        return redirectUrl;
    }

    @Override
    public String processSLO(String profile, HttpServletRequest request, HttpServletResponse response) {
        try {
            HttpRequest httpRequest = ServletUtils.makeHttpRequest(request);
            String samlRequestParameter = httpRequest.getParameter("SAMLRequest");
            String samlResponseParameter = httpRequest.getParameter("SAMLResponse");

            Saml2Settings settings = OneLoginSAMLSettingUtils.buildSettings(profile);

            if (samlRequestParameter == null && samlResponseParameter == null) {
                throw new RuntimeException("SAML LogoutRequest/LogoutResponse not found. " +
                        "Only supported HTTP_REDIRECT Binding");
            } else if (samlResponseParameter != null) {
                processSLOResponse(settings, httpRequest);
            } else {
                return processSLORequest(settings, httpRequest);
            }

        } catch (Exception e) {
            log.warn("Failed to process SLO request.", e);
        }

        String relayState = request.getParameter("RelayState");
        String baseUrl = StringUtils.isBlank(relayState) ? OneLoginSAMLSettingUtils.getLoginUrl(profile) : relayState;
        return String.format("%s/login/form", baseUrl);
    }

    private void processSLOResponse(Saml2Settings settings, HttpRequest httpRequest) {
        LogoutResponse logoutResponse = new LogoutResponse(settings, httpRequest);
        if (!logoutResponse.isValid(null)) {
            throw new RuntimeException("processSLO error. invalid_logout_response");
        } else {
            try {
                String inResponseTo = logoutResponse.getStatus();
                if (inResponseTo != null && inResponseTo.equals(Constants.STATUS_SUCCESS)) {
                    String lastMessageId = logoutResponse.getId();
                    log.info("Logout response is successful, lastMessageId=" + lastMessageId);
                } else {
                    throw new RuntimeException("processSLO error. logout_not_success");
                }
            } catch (XPathExpressionException e) {
                throw new RuntimeException("Failed to path SAML logout response", e);
            }
        }
    }

    private String processSLORequest(Saml2Settings settings, HttpRequest httpRequest) throws Exception {
        LogoutRequest logoutRequest = new LogoutRequest(settings, httpRequest);
        if (!logoutRequest.isValid()) {
            throw new RuntimeException("processSLO error. invalid_logout_request");
        } else {
            try {
                Document samlRequestDoc = SAMLParsingUtils.parseSAMLRequest(httpRequest.getParameter("SAMLRequest"));
                String issuer = SAMLParsingUtils.extractIssuer(samlRequestDoc);
                String nameId = SAMLParsingUtils.extractNameID(samlRequestDoc);
                log.info("Received a SLO request for " + nameId + " from the issuer " + issuer);
                sessionService.logoutByIdp(nameId, issuer);
            } catch (Exception e) {
                log.warn("Failed to process SLO request", e);
            }

            LogoutResponse logoutResponseBuilder = new LogoutResponse(settings, httpRequest);
            logoutResponseBuilder.build(logoutRequest.id);
            String samlLogoutResponse = logoutResponseBuilder.getEncodedLogoutResponse();
            Map<String, String> parameters = new LinkedHashMap<>();
            parameters.put("SAMLResponse", samlLogoutResponse);
            String relayState = httpRequest.getParameter("RelayState");
            if (StringUtils.isNotBlank(relayState)) {
                parameters.put("RelayState", relayState);
            }
            if (settings.getLogoutResponseSigned()) {
                String sigAlg = settings.getSignatureAlgorithm();
                String signature = buildResponseSignature(settings, samlLogoutResponse, relayState, sigAlg);
                parameters.put("SigAlg", sigAlg);
                parameters.put("Signature", signature);
            }
            String sloUrl = settings.getIdpSingleLogoutServiceResponseUrl().toString();
            log.info("Logout response sent to " + sloUrl + " --> " + samlLogoutResponse);
            return constructUrl(sloUrl, parameters);
        }
    }

    @Override
    public String login(String redirectTo, HttpServletRequest request, HttpServletResponse response) {
        String baseUrl = StringUtils.isBlank(redirectTo) ? OneLoginSAMLSettingUtils.getLoginUrl(profile) : redirectTo;
        try {
            Saml2Settings settings = OneLoginSAMLSettingUtils.buildSettings(profile);
            AuthnRequest authnRequest = new AuthnRequest(settings, false, false, true, null);
            String samlRequest = authnRequest.getEncodedAuthnRequest();
            log.info("Generated SAMLRequest: " + authnRequest.getAuthnRequestXml());
            Map<String, String> parameters = new HashMap<>();
            parameters.put("SAMLRequest", samlRequest);
            if (StringUtils.isNotBlank(redirectTo)) {
                parameters.put("RelayState", redirectTo);
            }
            if (settings.getAuthnRequestsSigned()) {
                String algorithm = settings.getSignatureAlgorithm();
                String signature = buildRequestSignature(settings, samlRequest, redirectTo, algorithm);
                parameters.put("SigAlg", algorithm);
                parameters.put("Signature", signature);
            }
            String ssoUrl = settings.getIdpSingleSignOnServiceUrl().toString();
            return constructUrl(ssoUrl, parameters);
        } catch (Exception e) {
            log.warn("Failed to send OneLogin SAML request.", e);
            return String.format("%s/login/error", baseUrl);
        }
    }

    @Override
    public String logout(OneLoginExternalSession externalSession, String redirectTo) {
        String profile = externalSession.getProfile();
        if (!OneLoginSAMLSettingUtils.isSLOEnabled(profile)) {
            log.info("SLO is not enabled for sp profile " + profile);
            return null;
        }

        String baseUrl = StringUtils.isBlank(redirectTo) ? OneLoginSAMLSettingUtils.getLoginUrl(profile) : redirectTo;
        Saml2Settings settings = OneLoginSAMLSettingUtils.buildSettings(profile);

        try {
            String nameId = externalSession.getNameId();
            String nameIdFormat = externalSession.getNameIdFormat();
            String nameIdNameQualifier = externalSession.getNameIdNameQualifier();
            String nameIdSPNameQualifier = externalSession.getNameIdSPNameQualifier();
            String sessionIndex = externalSession.getSessionIndex();

            Map<String, String> parameters = new HashMap<>();
            LogoutRequest logoutRequest = new LogoutRequest(settings, null, nameId, sessionIndex, nameIdFormat,
                    nameIdNameQualifier, nameIdSPNameQualifier);
            String samlLogoutRequest = logoutRequest.getEncodedLogoutRequest();
            parameters.put("SAMLRequest", samlLogoutRequest);
            if (StringUtils.isNotBlank(redirectTo)) {
                parameters.put("RelayState", redirectTo);
            }

            if (settings.getLogoutRequestSigned()) {
                String signAlg = settings.getSignatureAlgorithm();
                String signature = buildRequestSignature(settings, samlLogoutRequest, redirectTo, signAlg);
                parameters.put("SigAlg", signAlg);
                parameters.put("Signature", signature);
            }

            String sloUrl = settings.getIdpSingleLogoutServiceUrl().toString();
            log.info("Redirect logout request OneLogin session " + sessionIndex + " of " + nameId + " to " + sloUrl);
            return constructUrl(sloUrl, parameters);
        } catch (Exception e) {
            log.warn("Failed to logout OneLogin session " + JsonUtils.serialize(externalSession), e);
            return String.format("%s/login/error", baseUrl);
        }
    }


    private String buildRequestSignature(Saml2Settings settings, String samlMessage, String relayState, String signAlg)
            throws SettingsException, IllegalArgumentException {
        return buildSignature(settings, samlMessage, relayState, signAlg, "SAMLRequest");
    }

    private String buildResponseSignature(Saml2Settings settings, String samlMessage, String relayState, String signAlg)
            throws SettingsException, IllegalArgumentException {
        return buildSignature(settings, samlMessage, relayState, signAlg, "SAMLResponse");
    }

    /**
     * Copied from the com.onelogin.saml2.Auth
     */
    private String buildSignature(Saml2Settings settings, String samlMessage, String relayState, String signAlg, String type)
            throws SettingsException, IllegalArgumentException {
        String signature = "";
        if (!settings.checkSPCerts()) {
            String errorMsg = "Trying to sign the " + type + " but can't load the SP private key";
            log.error("buildSignature error. " + errorMsg);
            throw new SettingsException(errorMsg, 4);
        } else {
            PrivateKey key = settings.getSPkey();
            String msg = type + "=" + Util.urlEncoder(samlMessage);
            if (StringUtils.isNotEmpty(relayState)) {
                msg = msg + "&RelayState=" + Util.urlEncoder(relayState);
            }

            if (StringUtils.isBlank(signAlg)) {
                signAlg = Constants.RSA_SHA1;
            }

            msg = msg + "&SigAlg=" + Util.urlEncoder(signAlg);

            try {
                signature = Util.base64encoder(Util.sign(msg, key, signAlg));
            } catch (NoSuchAlgorithmException | SignatureException | InvalidKeyException var10) {
                String errorMsg = "buildSignature error." + var10.getMessage();
                log.error(errorMsg);
            }

            if (signature.isEmpty()) {
                String errorMsg = "There was a problem when calculating the Signature of the " + type;
                log.error("buildSignature error. " + errorMsg);
                throw new IllegalArgumentException(errorMsg);
            } else {
                log.info("buildResponseSignature success. --> " + signature);
                return signature;
            }
        }
    }

    private String constructUrl(String url, Map<String, String> params) {
        if (MapUtils.isNotEmpty(params)) {
            List<String> bindings = new ArrayList<>();
            params.forEach((k, v) -> {
                try {
                    String binding = String.format("%s=%s", k, URLEncoder.encode(v, "UTF-8"));
                    bindings.add(binding);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException("Failed to url encode " + v);
                }
            });
            url += "?" + StringUtils.join(bindings, "&");
        }
        return url;
    }

}
