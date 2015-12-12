package com.latticeengines.security.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.LoginDocument.LoginResult;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.pls.UserDocument.UserResult;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.ResetPasswordRequest;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.RightsUtilities;
import com.latticeengines.security.exposed.exception.LoginException;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.EmailService;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "login", description = "REST resource for logging in using Lattice Global Auth")
@RestController
public class LoginResource {

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private SessionService sessionService;

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Autowired
    private EmailService emailService;

    @Autowired
    private UserService userService;

    @Autowired
    private TenantService tenantService;

    @RequestMapping(value = "/login", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Login to Lattice external application")
    public LoginDocument login(@RequestBody Credentials creds) {
        LoginDocument doc = new LoginDocument();

        try {
            Ticket ticket = globalAuthenticationService.authenticateUser(
                    creds.getUsername().toLowerCase(),
                    creds.getPassword());

            doc.setRandomness(ticket.getRandomness());
            doc.setUniqueness(ticket.getUniqueness());
            doc.setSuccess(true);

            LoginResult result = doc.new LoginResult();
            result.setMustChangePassword(ticket.isMustChangePassword());
            result.setPasswordLastModified(ticket.getPasswordLastModified());
            List<Tenant> tenants = new ArrayList<>(ticket.getTenants());
            tenants.retainAll(tenantService.getAllTenants());
            Collections.sort(tenants, new TenantNameSorter());
            result.setTenants(tenants);
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
            Ticket ticket = new Ticket(request.getHeader(Constants.AUTHORIZATION));
            ticket.setTenants(Collections.singletonList(tenant));
            doc.setTicket(ticket);

            Session session = sessionService.attach(ticket);
            doc.setSuccess(true);

            UserResult result = doc.new UserResult();
            UserResult.User user = result.new User();
            user.setDisplayName(session.getDisplayName());
            user.setEmailAddress(session.getEmailAddress());
            user.setIdentifier(session.getIdentifier());
            user.setLocale(session.getLocale());
            user.setTitle(session.getTitle());
            user.setAvailableRights(RightsUtilities.translateRights(session.getRights()));
            user.setAccessLevel(session.getAccessLevel());
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

    @RequestMapping(value = "/forgotpassword", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Reset password and send an email")
    public boolean forgotPassword(@RequestBody ResetPasswordRequest request) {
        String username = request.getUsername();
        String tempPass = globalUserManagementService.resetLatticeCredentials(username);

        if (LatticeProduct.LPA.equals(request.getProduct())) {
            User user = userService.findByUsername(username);
            String host = request.getHostPort();
            emailService.sendPlsForgetPasswordEmail(user, tempPass, host);
        }

        return true;
    }

    @RequestMapping(value = "/logout", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Logout the user")
    public SimpleBooleanResponse logout(HttpServletRequest request) {
        String token = request.getHeader(request.getHeader(Constants.AUTHORIZATION));
        if (StringUtils.isNotEmpty(token)) {
            sessionService.logout(new Ticket(token));
        }
        return SimpleBooleanResponse.successResponse();
    }


    class TenantNameSorter implements Comparator<Tenant> {

        public int compare(Tenant oneTenant, Tenant anotherTenant) {
            return oneTenant.getName().compareTo(anotherTenant.getName());
        }

    }
}