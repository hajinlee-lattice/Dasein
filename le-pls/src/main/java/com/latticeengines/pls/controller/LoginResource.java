package com.latticeengines.pls.controller;

import java.util.Arrays;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.LoginDocument.LoginResult;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.pls.UserDocument.UserResult;
import com.latticeengines.domain.exposed.pls.UserDocument.UserResult.User;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.pls.exception.LoginException;
import com.latticeengines.pls.globalauth.authentication.GlobalAuthenticationService;
import com.latticeengines.pls.globalauth.authentication.GlobalSessionManagementService;
import com.latticeengines.pls.globalauth.authentication.GlobalUserManagementService;
import com.latticeengines.pls.security.RestGlobalAuthenticationFilter;
import com.latticeengines.pls.security.RightsUtilities;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "login", description = "REST resource for logging in")
@RestController
public class LoginResource {

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private GlobalSessionManagementService globalSessionManagementService;

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @RequestMapping(value = "/login", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Login to the PLS application")
    public LoginDocument login(@RequestBody Credentials creds, HttpServletResponse response) {
        LoginDocument doc = new LoginDocument();

        try {
            Ticket ticket = globalAuthenticationService.authenticateUser(creds.getUsername(), creds.getPassword());

            doc.setRandomness(ticket.getRandomness());
            doc.setUniqueness(ticket.getUniqueness());
            doc.setSuccess(true);

            LoginResult result = doc.new LoginResult();
            result.setMustChangePassword(ticket.isMustChangePassword());
            result.setTenants(ticket.getTenants());
            doc.setResult(result);
        } catch (LedpException e) {
            if (e.getCode() == LedpCode.LEDP_18001) {
                throw new LoginException(e);
            }
            throw e;
        }

        return doc;
    }

    @RequestMapping(value = "/attach", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Attach the tenant")
    public UserDocument attach(@RequestBody Tenant tenant, HttpServletRequest request) {
        UserDocument doc = new UserDocument();

        try {
            Ticket ticket = new Ticket(request.getHeader(RestGlobalAuthenticationFilter.AUTHORIZATION));
            ticket.setTenants(Arrays.asList(tenant));
            doc.setTicket(ticket);

            Session session = globalSessionManagementService.attach(ticket);
            doc.setSuccess(true);

            UserResult result = doc.new UserResult();
            User user = result.new User();
            user.setDisplayName(session.getDisplayName());
            user.setEmailAddress(session.getEmailAddress());
            user.setIdentifier(session.getIdentifier());
            user.setLocale(session.getLocale());
            user.setTitle(session.getTitle());
            user.setAvailableRights(RightsUtilities.translateRights(session.getRights()));
            result.setUser(user);

            doc.setResult(result);
        } catch (LedpException e) {
            if (e.getCode() == LedpCode.LEDP_18001) {
                throw new LoginException(e);
            }
            throw e;
        }
        return doc;
    }

    @RequestMapping(value = "/forgotpassword/{userName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Reset password and send an email")
    public Boolean forgotPassword(@PathVariable String userName) {

        //TODO:[13Feb2015] GlobalAuth requires a deployment Id. UI does not require such an input. Using a fake one.
        Ticket ticket = globalAuthenticationService.authenticateUser("admin", DigestUtils.sha256Hex("admin"));
        String tenantId = ticket.getTenants().get(0).getId();

        return globalUserManagementService.forgotLatticeCredentials(userName, tenantId);
    }
}
