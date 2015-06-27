package com.latticeengines.pls.controller;

import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.pls.service.TenantService;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.InternalResourceBase;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.UserService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "admin", description = "REST resource for managing PLS tenants")
@RestController
@RequestMapping(value = "/admin")
public class AdminResource extends InternalResourceBase {
    private static final Log LOGGER = LogFactory.getLog(InternalResource.class);

    @Autowired
    private TenantService tenantService;
    
    @Autowired
    private UserService userService;

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @RequestMapping(value = "/tenants", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Add a PLS tenant")
    public Boolean addTenant(@RequestBody Tenant tenant, HttpServletRequest request) {
        checkHeader(request);
        if (!tenantService.hasTenantId(tenant.getId())) {
            tenantService.registerTenant(tenant);
        } else {
            tenantService.updateTenant(tenant);
        }
        return true;
    }

    @RequestMapping(value = "/tenants", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of registered tenants")
    public List<Tenant> getTenants(HttpServletRequest request) {
        checkHeader(request);
        return tenantService.getAllTenants();
    }

    @RequestMapping(value = "/tenants/{tenantId:.+}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a tenant.")
    public Boolean deleteTenant(@PathVariable String tenantId,
                                @RequestParam(value = "tenantName", required = false, defaultValue = " ") String tenantName,
                                HttpServletRequest request) {
        checkHeader(request);
        Tenant tenant = new Tenant();
        tenant.setName(tenantName);
        tenant.setId(tenantId);
        tenantService.discardTenant(tenant);
        return true;
    }
    
    @RequestMapping(value = "/users", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Add a PLS admin user")
    public Boolean addAdminUser(@RequestBody UserRegistrationWithTenant userRegistrationWithTenant, HttpServletRequest request) {
        checkHeader(request);
        return userService.addAdminUser(userRegistrationWithTenant);
    }

    @RequestMapping(value = "/users", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update users. Mainly for upgrade from old GrantedRights to new AccessLevel.")
    public SimpleBooleanResponse updateUsers(
            @RequestParam(value = "username") String username,
            @RequestParam(value = "tenant") String tenantId,
            @RequestBody UserUpdateData userUpdateData,
            HttpServletRequest request
    ) throws URIException {
        checkHeader(request);

        username = URIUtil.decode(username);
        tenantId = URIUtil.decode(tenantId);

        if(userService.findByUsername(username) == null) {
            return SimpleBooleanResponse.getFailResponse(Collections.singletonList(
                    String.format("User %s does not exist.", username)));
        }

        if(tenantService.findByTenantId(tenantId) == null) {
            return SimpleBooleanResponse.getFailResponse(Collections.singletonList(
                    String.format("Tenant %s does not exist.", tenantId)));
        }

        AccessLevel accessLevel = null;
        if (userUpdateData.getAccessLevel() != null) {
            accessLevel = AccessLevel.valueOf(userUpdateData.getAccessLevel());
        }
        String oldPassword = null;
        if (userUpdateData.getOldPassword() != null) {
            oldPassword = DigestUtils.sha256Hex(userUpdateData.getOldPassword());
        }
        String newPassword = null;
        if (userUpdateData.getNewPassword() != null) {
            newPassword = DigestUtils.sha256Hex(userUpdateData.getNewPassword());
        }

        LOGGER.info(String.format("Updating user %s in the tenant %s using the internal API", username, tenantId));

        if (accessLevel != null) {
            userService.assignAccessLevel(accessLevel, tenantId, username);
            LOGGER.info(String.format(
                    "User %s has been updated to %s for the tenant %s through the internal API",
                    username, accessLevel.name(), tenantId
            ));
        }

        if (oldPassword != null && newPassword != null) {
            Ticket ticket = globalAuthenticationService.authenticateUser(username, oldPassword);

            Credentials oldCreds = new Credentials();
            oldCreds.setUsername(username);
            oldCreds.setPassword(oldPassword);

            Credentials newCreds = new Credentials();
            newCreds.setUsername(username);
            newCreds.setPassword(newPassword);
            globalUserManagementService.modifyLatticeCredentials(ticket, oldCreds, newCreds);

            LOGGER.info(String.format("The password of user %s has been updated through the internal API", username));

            globalAuthenticationService.discard(ticket);
        }

        return SimpleBooleanResponse.getSuccessResponse();
    }

}
