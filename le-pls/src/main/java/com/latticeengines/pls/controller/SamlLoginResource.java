package com.latticeengines.pls.controller;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HttpHeaders;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.pls.UserDocument.UserResult;
import com.latticeengines.domain.exposed.saml.LoginValidationResponse;
import com.latticeengines.domain.exposed.saml.LogoutValidationResponse;
import com.latticeengines.domain.exposed.saml.SamlConfigMetadata;
import com.latticeengines.domain.exposed.saml.SpSamlLoginRequest;
import com.latticeengines.domain.exposed.saml.SpSamlLoginResponse;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.proxy.exposed.saml.SPSamlProxy;
import com.latticeengines.proxy.exposed.saml.SamlConfigProxy;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.RightsUtilities;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.service.UserService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "SAML Login", description = "REST resource for logging in using SAML Auth")
@RestController
@RequestMapping(path = "/saml")
public class SamlLoginResource {

    private static final String RETURN_TO_PARAM = "return_to";

    private static final Logger log = LoggerFactory.getLogger(SamlLoginResource.class);

    @Autowired
    private SessionService sessionService;

    @Autowired
    private UserService userService;

    @Autowired
    private SPSamlProxy samlProxy;

    @Autowired
    private SamlConfigProxy samlConfigProxy;

    @Value("${security.app.public.url:https://localhost:3000}")
    private String loginUrl;

    @Value("${pls.saml.local.redirection.allowed:false}")
    private boolean isLocalRedirectionAllowed;

    @RequestMapping(value = "/login/"
            + InternalResource.TENANT_ID_PATH, method = RequestMethod.POST, consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
    @ResponseBody
    @ApiOperation(value = "Login via SAML Authentication")
    public RedirectView authenticateSamlUserAndAttachTenant(@PathVariable("tenantId") String tenantDeploymentId,
            @RequestParam("SAMLResponse") String samlResponse,
            @RequestParam(name = "RelayState", required = false) String relayState,
            // TODO - remove this once UI integration is stable. This is added
            // to help UI to point to QA backend but still get call to local
            // login UI
            @RequestParam(name = "enforceLocalUI", required = false, defaultValue = "false") boolean enforceLocalUI,
            @RequestParam(name = "testUserId", required = false, defaultValue = "false") String testUserId) {
        RedirectView redirectView = new RedirectView();
        String baseLoginURL = loginUrl;

        try {
            UserDocument uDoc = new UserDocument();
            log.info(
                    String.format("SAML Login Resource: TenantDeploymentId = %s , RelayState = %s , SAMLResponse = %s ",
                            tenantDeploymentId, relayState, samlResponse));

            LoginValidationResponse samlLoginResp = null;

            if (isLocalRedirectionAllowed && enforceLocalUI) {
                // for UI dev - local redirection is allowed only for QA and
                // local env. For prod this is not allowed.
                baseLoginURL = "https://localhost:3000";
                samlLoginResp = new LoginValidationResponse();
                String testUserName = StringUtils.isBlank(testUserId) ? "bnguyen@test-domain.com" : testUserId;
                samlLoginResp.setUserId(testUserName);
                samlLoginResp.setValidated(true);
            } else {
                samlLoginResp = samlProxy.validateSSOLogin(tenantDeploymentId, samlResponse, relayState);
            }

            if (!samlLoginResp.isValidated()) {
                log.warn(LedpCode.LEDP_18170.toString() + " Info: " + samlLoginResp.toString());
                redirectView.setUrl(String.format("%s/login/saml/%s/error", baseLoginURL, tenantDeploymentId));
                return redirectView;
            }

            sessionService.validateSamlLoginResponse(samlLoginResp);

            /*
            //TODO: Commenting part of PLS-7954. Need to be enabled after we fix the allowed domains check/
            // Check whether User already exists in the system and other attributes are up to date too.
            if (StringUtils.isBlank(samlLoginResp.getFirstName())) {
                //TODO: Anoop. Delete this code after Saml response is parsed with correct values
                log.warn("Need to remove these hard coded values.");
                samlLoginResp.setFirstName("Test FirstName");
                samlLoginResp.setLastName("Test LastName");
                List<String> externalRoles = new ArrayList<>();
                externalRoles.add(SamlIntegrationRole.LATTICE_USER.name());
                samlLoginResp.setUserRoles(externalRoles);
            }
            userService.upsertSamlIntegrationUser(samlLoginResp, tenantDeploymentId);
            */
            String userName = samlLoginResp.getUserId();
            Session session = sessionService.attachSamlUserToTenant(userName, tenantDeploymentId);

            uDoc.setSuccess(true);
            uDoc.setAuthenticationRoute(session.getAuthenticationRoute());
            uDoc.setTicket(session.getTicket());

            UserResult result = getUserResult(uDoc, session);

            uDoc.setResult(result);

            Map<String, Object> attributeMap = new HashMap<>();
            attributeMap.put("userName", uDoc.getResult().getUser().getEmailAddress());
            attributeMap.put("samlAuthenticated", true);
            attributeMap.put("userDocument", JsonUtils.serialize(uDoc));
            if (StringUtils.isNotBlank(relayState)) {
                redirectView.setUrl(String.format("%s/login/saml/%s?%s", baseLoginURL, tenantDeploymentId, relayState));
            } else {
                redirectView.setUrl(String.format("%s/login/saml/%s", baseLoginURL, tenantDeploymentId));
            }
            redirectView.setAttributesMap(attributeMap);

        } catch (LedpException e) {
            log.info(e.getMessage(), e);
            redirectView.setUrl(String.format("%s/login/saml/%s/error", baseLoginURL, tenantDeploymentId));
        }

        return redirectView;
    }

    protected UserResult getUserResult(UserDocument uDoc, Session session) {
        UserResult result = uDoc.new UserResult();
        UserResult.User user = result.new User();
        user.setDisplayName(session.getDisplayName());
        user.setEmailAddress(session.getEmailAddress());
        user.setIdentifier(session.getIdentifier());
        user.setLocale(session.getLocale());
        user.setTitle(session.getTitle());
        user.setAvailableRights(RightsUtilities.translateRights(session.getRights()));
        user.setAccessLevel(session.getAccessLevel());
        result.setUser(user);
        return result;
    }

    @RequestMapping(value = "/logout/"
            + InternalResource.TENANT_ID_PATH, method = RequestMethod.POST, consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
    @ResponseBody
    @ApiOperation(value = "Logout the user at GA and SAML IDP")
    public SimpleBooleanResponse logoutFromSpAndIDP(HttpServletRequest request,
            @PathVariable("tenantId") String tenantDeploymentId, @RequestParam("SAMLResponse") String samlResponse,
            @RequestParam(name = "isSPInitiatedLogout", required = false) String isSPInitiatedLogout) {
        log.info("SAML Logout Resource: TenantDeploymentId - isSPInitiatedLogout - Response ", tenantDeploymentId,
                samlResponse, isSPInitiatedLogout);

        String token = request.getHeader(Constants.AUTHORIZATION);
        if (StringUtils.isNotEmpty(token)) {
            sessionService.logout(new Ticket(token));
        }
        LogoutValidationResponse samlLogoutResp = samlProxy.validateSingleLogout(tenantDeploymentId,
                Boolean.valueOf(isSPInitiatedLogout), samlResponse);
        return SimpleBooleanResponse.successResponse();
    }

    /*
     * @RequestMapping(value = "/metadata/" + InternalResource.TENANT_ID_PATH,
     * method = RequestMethod.GET, headers = "Accept=application/json")
     *
     * @ResponseBody
     *
     * @ApiOperation(value =
     * "Get ServiceProvider SAML Metadata for requested Tenant")
     */
    public void getIdpMetadata(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("tenantId") String tenantDeploymentId) {
        log.info("SAML Metadata Resource: TenantDeploymentId ", tenantDeploymentId);

        try (ServletOutputStream outputStream = response.getOutputStream()) {
            String metadata = samlProxy.getSPMetadata(tenantDeploymentId);
            response.setContentType(MediaType.APPLICATION_XML_VALUE);
            response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);
            response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
                    String.format("attachment; filename=\"lattice_saml_sp_%s.xml\"", tenantDeploymentId));

            response.setStatus(HttpStatus.SC_OK);
            outputStream.write(metadata.getBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @RequestMapping(value = "/splogin", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    @ApiOperation(value = "Lattice initiated SAML Authentication and Login")
    public Map<String, SpSamlLoginResponse> spInitiateLoginRequest(@RequestBody SpSamlLoginRequest spLoginRequest) {
        log.info("Initiating SP Login for Request: %s", JsonUtils.serialize(spLoginRequest));

        String tenantDeploymentId = spLoginRequest.getTenantDeploymentId();
        if (StringUtils.isBlank(tenantDeploymentId)) {
            throw new LedpException(LedpCode.LEDP_33005);
        }
        SamlConfigMetadata samlConfigMetadata = samlConfigProxy.getSamlConfigMetadata(tenantDeploymentId);
        if (StringUtils.isBlank(samlConfigMetadata.getSingleSignOnService())) {
            throw new LedpException(LedpCode.LEDP_33006);
        }
        String encodedRelayState = encode(constructRelayState(spLoginRequest));
        String ssoUrl = String.format("%s?RelayState=%s", samlConfigMetadata.getSingleSignOnService(), encodedRelayState);
        log.info("SSO Url for Sp Initiated Login: %s", ssoUrl);
        SpSamlLoginResponse spResp = new SpSamlLoginResponse();
        spResp.setSsoUrl(ssoUrl);
        return ImmutableMap.of(SpSamlLoginResponse.class.getSimpleName(), spResp);
    }

    private String encode(String constructRelayState) {
        if (constructRelayState == null) {
            return null;
        }
        try {
            return URLEncoder.encode(constructRelayState, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Error while encoding RelayState");
        }
    }

    public String constructRelayState(SpSamlLoginRequest spLoginRequest) {
        if(spLoginRequest == null || spLoginRequest.getRequestParameters() == null) {
            return "";
        }
        Map<String, String> reqParams = new HashMap<>(spLoginRequest.getRequestParameters());

        // ReturnTo needs to be last parameter in RelayStateURL, Because it could contain additional query parameters
        String returnToValue = reqParams.get(RETURN_TO_PARAM);
        reqParams.remove(RETURN_TO_PARAM);
        if (StringUtils.isBlank(returnToValue)) {
            return "";
        }
        StringBuffer sb = new StringBuffer();
        reqParams.keySet().forEach(key -> {
            sb.append(key).append("=").append(reqParams.get(key)).append("&");
        });
        sb.append(RETURN_TO_PARAM).append("=").append(returnToValue);

        return sb.toString();
    }

}
