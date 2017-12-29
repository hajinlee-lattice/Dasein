package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.LoginException;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.LoginDocument.LoginResult;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.pls.UserDocument.UserResult;
import com.latticeengines.domain.exposed.saml.LoginValidationResponse;
import com.latticeengines.domain.exposed.saml.LogoutValidationResponse;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.proxy.exposed.saml.SPSamlProxy;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.RightsUtilities;
import com.latticeengines.security.exposed.service.SessionService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "SAML Login", description = "REST resource for logging in using SAML Auth")
@RestController
@RequestMapping(path = "/saml")
public class SamlLoginResource {

    private static final Logger log = LoggerFactory.getLogger(SamlLoginResource.class);

    @Autowired
    private SessionService sessionService;

    @Autowired
    private SPSamlProxy samlProxy;

    @Value("${security.app.public.url:https://localhost:3000}")
    private String loginUrl;

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
            @RequestParam(name = "enforceLocalUI", required = false, defaultValue = "false") boolean enforceLocalUI) {
        UserDocument uDoc = new UserDocument();
        RedirectView redirectView = new RedirectView();
        try {
            @SuppressWarnings("unchecked")
            MultivaluedMap<String, String> formParams = null;
            log.info("SAML Login Resource: TenantDeploymentId - RelayState - Response ", tenantDeploymentId, relayState,
                    samlResponse);

            LoginValidationResponse samlLoginResp = samlProxy.validateSSOLogin(tenantDeploymentId, samlResponse,
                    relayState);

            if (!samlLoginResp.isValidated()) {
                throw new LedpException(LedpCode.LEDP_18170);
            }
            String userName = samlLoginResp.getUserId();

            Session session = sessionService.attchSamlUserToTenant(userName, tenantDeploymentId);

            uDoc.setSuccess(true);
            uDoc.setAuthenticationRoute(session.getAuthenticationRoute());
            uDoc.setTicket(session.getTicket());

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

            uDoc.setResult(result);

            LoginDocument lDoc = new LoginDocument();
            lDoc.setErrors(new ArrayList<>());
            lDoc.setRandomness(session.getTicket().getRandomness());
            lDoc.setUniqueness(session.getTicket().getUniqueness());
            lDoc.setSuccess(true);
            LoginResult loginResult = lDoc.new LoginResult();
            loginResult.setMustChangePassword(false);
            loginResult.setPasswordLastModified(0L);
            loginResult.setTenants(session.getTicket().getTenants());
            lDoc.setResult(loginResult);
            lDoc.setAuthenticationRoute(session.getAuthenticationRoute());

            Map<String, Object> attributeMap = new HashMap<>();
            attributeMap.put("userName", uDoc.getResult().getUser().getEmailAddress());
            attributeMap.put("samlAuthenticated", true);
            attributeMap.put("userDocument", JsonUtils.serialize(uDoc));
            attributeMap.put("loginDocument", JsonUtils.serialize(lDoc));

            String baseLoginURL = loginUrl;
            if (enforceLocalUI) {
                baseLoginURL = "https://localhost:3000";
            }
            redirectView.setUrl(String.format("%s/login/saml/%s", baseLoginURL, tenantDeploymentId));
            redirectView.setAttributesMap(attributeMap);

        } catch (LedpException e) {
            if (e.getCode() == LedpCode.LEDP_18170) {
                throw new LoginException(e);
            }
            throw e;
        }

        return redirectView;
    }

    @RequestMapping(value = "/attachUser/"
            + InternalResource.TENANT_ID_PATH, method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Login via SAML Authentication")
    public UserDocument attachUserToGASession(@PathVariable("tenantId") String tenantDeploymentId,
            @QueryParam("UserName") String userName) {
        UserDocument uDoc = new UserDocument();
        try {
            log.info("SAML Attach Resource: TenantDeploymentId - UserName ", tenantDeploymentId, userName);

            Session session = sessionService.attchSamlUserToTenant(userName, tenantDeploymentId);

            uDoc.setSuccess(true);
            uDoc.setAuthenticationRoute(session.getAuthenticationRoute());

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

            uDoc.setResult(result);

        } catch (LedpException e) {
            if (e.getCode() == LedpCode.LEDP_18170) {
                throw new LoginException(e);
            }
            throw e;
        }

        return uDoc;
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

    @RequestMapping(value = "/metadata/"
            + InternalResource.TENANT_ID_PATH, method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get ServiceProvider SAML Metadata for requested Tenant")
    public Response getIdpMetadata(HttpServletRequest request, @PathVariable("tenantId") String tenantDeploymentId) {
        log.info("SAML Metadata Resource: TenantDeploymentId ", tenantDeploymentId);

        String metadata = samlProxy.getSPMetadata(tenantDeploymentId);
        return Response.ok().entity(metadata).build();
    }

}
