package com.latticeengines.pls.controller;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.LoginException;
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
@RestController("/saml")
public class SamlLoginResource {

    private static final Logger log = LoggerFactory.getLogger(SamlLoginResource.class);

    @Autowired
    private SessionService sessionService;

    @Autowired
    private SPSamlProxy samlProxy;

    @RequestMapping(value = "/login/{tenantDeploymentId}", method = RequestMethod.POST, headers = "Accept=application/json", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
    @ResponseBody
    @ApiOperation(value = "Login via SAML Authentication")
    public UserDocument authenticateSamlUserAndAttachTenant(@PathVariable String tenantDeploymentId,
            @RequestBody MultivaluedMap<String, String> formParams) {
        UserDocument uDoc = new UserDocument();
        try {
            log.info("SAML Login Resource: TenantDeploymentId - RelayState - Response ", tenantDeploymentId,
                    formParams.getFirst("RelayState"), formParams.getFirst("SAMLResponse"));

            LoginValidationResponse samlLoginResp = samlProxy.validateSSOLogin(tenantDeploymentId,
                    formParams.getFirst("SAMLResponse"), formParams.getFirst("RelayState"));

            if (!samlLoginResp.isValidated()) {
                throw new LedpException(LedpCode.LEDP_18170);
            }
            String userName = samlLoginResp.getUserId();

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

    @RequestMapping(value = "/logout/{tenantDeploymentId}", method = RequestMethod.POST, headers = "Accept=application/json", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
    @ResponseBody
    @ApiOperation(value = "Logout the user at GA and SAML IDP")
    public SimpleBooleanResponse logoutFromSpAndIDP(HttpServletRequest request, @PathVariable String tenantDeploymentId,
            @RequestBody MultivaluedMap<String, String> formParams) {
        log.info("SAML Logout Resource: TenantDeploymentId - isSPInitiatedLogout - Response ", tenantDeploymentId,
                formParams.getFirst("isSPInitiatedLogout"), formParams.getFirst("SAMLResponse"));

        String token = request.getHeader(Constants.AUTHORIZATION);
        if (StringUtils.isNotEmpty(token)) {
            sessionService.logout(new Ticket(token));
        }
        LogoutValidationResponse samlLogoutResp = samlProxy.validateSingleLogout(tenantDeploymentId,
                Boolean.valueOf(formParams.getFirst("isSPInitiatedLogout")), formParams.getFirst("SAMLResponse"));
        return SimpleBooleanResponse.successResponse();
    }

    @RequestMapping(value = "/metadata/{tenantDeploymentId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get ServiceProvider SAML Metadata for requested Tenant")
    public Response getIdpMetadata(HttpServletRequest request, @PathVariable String tenantDeploymentId,
            @RequestBody MultivaluedMap<String, String> formParams) {
        log.debug("SAML Metadata Resource: TenantDeploymentId ", tenantDeploymentId);

        String metadata = samlProxy.getSPMetadata(tenantDeploymentId);
        return Response.ok().entity(metadata).build();
    }

}
