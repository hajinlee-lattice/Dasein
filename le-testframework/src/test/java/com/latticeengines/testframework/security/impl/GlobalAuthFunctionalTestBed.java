package com.latticeengines.testframework.security.impl;

import java.util.Arrays;
import java.util.Collections;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.RightsUtilities;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;
import com.latticeengines.testframework.security.GlobalAuthTestBed;

@Component("globalAuthFunctionalTestBed")
public class GlobalAuthFunctionalTestBed extends AbstractGlobalAuthTestBed implements GlobalAuthTestBed {

    private static Log log = LogFactory.getLog(GlobalAuthFunctionalTestBed.class);

    @Autowired
    private UserService userService;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private SessionService sessionService;

    @Override
    public void bootstrapForProduct(LatticeProduct product) {
        throw new UnsupportedOperationException("bootstrap for product is not applicable to functional tests.");
    }

    @Override
    public UserDocument loginAndAttach(String username, String password, Tenant tenant) {
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(DigestUtils.sha256Hex(password));

        Ticket ticket = sessionService.authenticate(creds);
        LoginDocument doc = new LoginDocument();
        doc.setRandomness(ticket.getRandomness());
        doc.setUniqueness(ticket.getUniqueness());

        Tenant tenant1 = tenantService.findByTenantId(tenant.getId());
        ticket.setTenants(Collections.singletonList(tenant1));

        UserDocument userDocument = new UserDocument();
        userDocument.setTicket(ticket);
        Session session = sessionService.attach(ticket);
        userDocument.setSuccess(true);
        UserDocument.UserResult result = userDocument.new UserResult();
        UserDocument.UserResult.User user = result.new User();
        user.setDisplayName(session.getDisplayName());
        user.setEmailAddress(session.getEmailAddress());
        user.setIdentifier(session.getIdentifier());
        user.setLocale(session.getLocale());
        user.setTitle(session.getTitle());
        user.setAvailableRights(RightsUtilities.translateRights(session.getRights()));
        user.setAccessLevel(session.getAccessLevel());
        result.setUser(user);

        userDocument.setResult(result);

        authHeaderInterceptor.setAuthValue(doc.getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { authHeaderInterceptor }));

        log.info("Log in user " + username + " to tenant " + tenant.getId() + " through service beans.");
        return userDocument;
    }

    @Override
    protected void bootstrapUser(AccessLevel accessLevel, Tenant tenant) {
        String username = TestFrameworkUtils.usernameForAccessLevel(accessLevel);
        if (userService.findByUsername(username) == null) {
            UserRegistration userReg = TestFrameworkUtils.createUserRegistration(accessLevel);
            userService.createUser(userReg);
        }
        userService.assignAccessLevel(accessLevel, tenant.getId(), username);
    }

    @Override
    protected void logout(UserDocument userDocument) {
        Ticket ticket = userDocument.getTicket();
        sessionService.logout(ticket);
    }

    @Override
    public void createTenant(Tenant tenant) {
        tenantService.registerTenant(tenant);
    }

    @Override
    public void deleteTenant(Tenant tenant) {
        tenantService.discardTenant(tenant);
    }

}
