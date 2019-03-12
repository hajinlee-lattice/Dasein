package com.latticeengines.security.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTicketEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserEntityMgr;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.LoginException;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.LoginDocument.LoginResult;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.pls.UserDocument.UserResult;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.ResetPasswordRequest;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.RightsUtilities;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "login", description = "REST resource for logging in using Lattice Global Auth")
@RestController
public class LoginResource {

    private static final Logger log = LoggerFactory.getLogger(LoginResource.class);

    @Value("${security.app.public.url}")
    private String APP_BASE_URL;

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

    @Autowired
    private GlobalAuthTicketEntityMgr gaTicketEntityMgr;

    @Autowired
    private GlobalAuthUserEntityMgr gaUserEntityMgr;

    @RequestMapping(value = "/login", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Login to Lattice external application")
    public LoginDocument login(@RequestBody Credentials creds) {
        LoginDocument doc = new LoginDocument();

        try {
            Ticket ticket = sessionService.authenticate(creds);

            if (ticket == null) {
                doc.setErrors(Collections.singletonList("The email address or password is not valid. Please re-enter your credentials."));
                return doc;
            }

            doc.setRandomness(ticket.getRandomness());
            doc.setUniqueness(ticket.getUniqueness());
            doc.setSuccess(true);

            GlobalAuthTicket ticketData = gaTicketEntityMgr.findByTicket(ticket.getData());
            GlobalAuthUser userData = gaUserEntityMgr.findByUserId(ticketData.getUserId());
            if (userData == null) {
                doc.setErrors(Collections.singletonList("The email address or password is not valid. Please re-enter your credentials."));
                return doc;
            }
            doc.setFirstName(userData.getFirstName());
            doc.setLastName(userData.getLastName());

            LoginResult result = doc.new LoginResult();
            result.setMustChangePassword(ticket.isMustChangePassword());
            result.setPasswordLastModified(ticket.getPasswordLastModified());
            List<Tenant> tenants = tenantService.getTenantsByStatus(TenantStatus.ACTIVE);
            List<Tenant> gaTenants = null;
            if (ticket.getTenants()!= null) {
                gaTenants = new ArrayList<>(ticket.getTenants());
            }
            if (gaTenants != null) {
                if (!gaTenants.isEmpty()) {
                    tenants.retainAll(gaTenants);
                    tenants.sort(new TenantNameSorter());
                    result.setTenants(tenants);
                }
            }
            doc.setResult(result);
        } catch (LedpException e) {
            doc.setErrors(Collections.singletonList(e.getCode().getMessage()));
        }
        return doc;
    }

    @RequestMapping(value = "/attach", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Attach the tenant")
    public UserDocument attach(@RequestBody Tenant tenant, HttpServletRequest request) {
        UserDocument doc = new UserDocument();

        try {
            String ticketData = request.getHeader(Constants.AUTHORIZATION);
            if (!StringUtils.isNotEmpty(ticketData)) {
                throw new LedpException(LedpCode.LEDP_18123);
            }
            Ticket ticket = new Ticket(ticketData);
            ticket.setTenants(Collections.singletonList(tenant));
            doc.setTicket(ticket);

            Session session = sessionService.attach(ticket);
            if (session == null) {
                // either ticket expired or not exist
                throw new LedpException(LedpCode.LEDP_18002, new String[] { ticket.getData() });
            }
            doc.setSuccess(true);
            doc.setAuthenticationRoute(session.getAuthenticationRoute());

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
            if (e.getCode() == LedpCode.LEDP_18001 || e.getCode() == LedpCode.LEDP_18002
                    || e.getCode() == LedpCode.LEDP_18123) {
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
        try {
            String username = request.getUsername();
            String tempPass = globalUserManagementService.resetLatticeCredentials(username);

            if (LatticeProduct.LPA.equals(request.getProduct())) {
                User user = userService.findByUsername(username);
                String host = request.getHostPort();
                emailService.sendPlsForgetPasswordEmail(user, tempPass, host);
            }
        } catch (Exception e) {
            log.error("Failed to reset password and send an email.", e);
        }

        return true;
    }

    @RequestMapping(value = "/logout", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Logout the user")
    public SimpleBooleanResponse logout(HttpServletRequest request) {
        String token = request.getHeader(Constants.AUTHORIZATION);
        if (StringUtils.isNotEmpty(token)) {
            // Make sure the token has at least two parts separated by a period
            // in order to generate a valid ticket.
            if (token.split("\\.").length > 1) {
                sessionService.logout(new Ticket(token));
                return SimpleBooleanResponse.successResponse();
            } else {
                log.warn("Invalid token (missing period) passed by HttpServletRequest to Logout.");
                // For now, return a successful response even though the token
                // was invalid. Based on the behavior of
                // login above, it looks like the proper response would be to
                // through an exception. However, throwing
                // exception is undesirable because they might trigger pager
                // duty. Returning a failure response was
                // considered but from looking at the UI code, unless a HTTP 401
                // Unauthorized response status code is
                // set, the UI doesn't properly clear the user state on a failed
                // response. It seemed the only way to
                // set the 401 code was to throw an exception so a failed
                // response won't do. Since the desired outcome
                // of an invalid token is to clear the user state and logout the
                // user which is the same as the behavior
                // for a valid token, we return a successful response to trigger
                // the proper logout action.
                // TODO: Reevaluate the proper response for this error condition
                // (return success, return failures, or
                // throw exception).
                return SimpleBooleanResponse.successResponse();
            }
        } else {
            log.warn("Logout request made with empty token.");
            // See above invalid token case as to why we return a successful
            // response here despite the empty token.
            return SimpleBooleanResponse.successResponse();
        }
    }

    @RequestMapping(value = "/password/{username:.+}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update password of user")
    public SimpleBooleanResponse updateCredentials(@PathVariable String username, @RequestBody UserUpdateData data,
            HttpServletRequest request) {
        User user = new User();
        user.setUsername(userService.getURLSafeUsername(username).toLowerCase());
        boolean updateSucceeded;
        // use this header for backward compatibility
        if (request.getHeader(Constants.PASSWORD_UPDATE_FORMAT_HEADERNAME) != null) {
            updateSucceeded = userService.updateClearTextCredentials(user, data);
        } else {
            // old format (sha256 hashed)
            updateSucceeded = userService.updateCredentials(user, data);
        }
        if (updateSucceeded) {
            return SimpleBooleanResponse.successResponse();
        } else {
            String message = "We could not verify your current password, please make sure you provide correct one.";
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(message));
        }

    }

    class TenantNameSorter implements Comparator<Tenant> {

        public int compare(Tenant oneTenant, Tenant anotherTenant) {
            return oneTenant.getName().compareTo(anotherTenant.getName());
        }

    }
}
