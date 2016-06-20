package com.latticeengines.security.exposed.globalauth.impl;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBElement;
import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.SSLUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.monitor.annotation.RestApiCall;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.globalauth.generated.service.AuthenticationService;
import com.latticeengines.security.globalauth.generated.service.IAuthenticationService;

@Component("globalAuthenticationService")
public class GlobalAuthenticationServiceImpl extends GlobalAuthenticationServiceBaseImpl implements GlobalAuthenticationService {

    private static final Log log = LogFactory.getLog(GlobalAuthenticationServiceImpl.class);

    @Override
    @RestApiCall
    public synchronized Ticket authenticateUser(String user, String password) {
        SSLUtils.turnOffSslChecking();

        AuthenticationService service;
        try {
            service = new AuthenticationService(new URL(globalAuthUrl + "/GlobalAuthService?wsdl"));
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18000, e, new String[] { globalAuthUrl });
        }

        IAuthenticationService ias = service.getBasicHttpBindingIAuthenticationService();
        addMagicHeaderAndSystemProperty(ias);
        try {
            log.info(String.format("Authenticating user %s against Global Auth.", user));
            return new TicketBuilder(ias.authenticateLattice(user, password)).build();
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18001, e, new String[] { user });
        }

    }

    @Override
    @RestApiCall
    public synchronized boolean discard(Ticket ticket) {
        SSLUtils.turnOffSslChecking();

        AuthenticationService service;
        try {
            service = new AuthenticationService(new URL(globalAuthUrl + "/GlobalAuthService?wsdl"));
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18000, e, new String[] { globalAuthUrl });
        }

        IAuthenticationService ias = service.getBasicHttpBindingIAuthenticationService();
        addMagicHeaderAndSystemProperty(ias);
        try {
            log.info("Discarding ticket " + ticket + " against Global Auth.");

            return ias.discard(new SoapTicketBuilder(ticket).build());
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18009, e, new String[] { ticket.toString() });
        }

    }

    static class SoapTicketBuilder {
        private Ticket ticket;

        public SoapTicketBuilder(Ticket ticket) {
            this.ticket = ticket;
        }

        public com.latticeengines.security.globalauth.generated.service.Ticket build() {
            com.latticeengines.security.globalauth.generated.service.Ticket t = new com.latticeengines.security.globalauth.generated.service.ObjectFactory().createTicket();
            t.setUniquness(ticket.getUniqueness());
            t.setRandomness(new JAXBElement<String>( //
                    new QName("http://schemas.lattice-engines.com/2008/Poet", "Randomness"), //
                    String.class, ticket.getRandomness()));
            return t;
        }
    }

    static class TicketBuilder {
        private com.latticeengines.security.globalauth.generated.service.Ticket ticket;

        public TicketBuilder(com.latticeengines.security.globalauth.generated.service.Ticket ticket) {
            this.ticket = ticket;
        }

        public Ticket build() {
            Ticket t = new Ticket();
            t.setMustChangePassword(ticket.isMustChangePassword());
            t.setRandomness(ticket.getRandomness().getValue());
            t.setUniqueness(ticket.getUniquness().intern());
            t.setPasswordLastModified(convertTicksToMilliseconds(ticket.getPasswordLastModifiedInTicks()));

            List<Tenant> tenants = new ArrayList<>();

            for (com.latticeengines.security.globalauth.generated.service.Tenant tenant : ticket.getTenants().getValue()
                    .getTenant()) {
                tenants.add(new TenantBuilder(tenant).build());
            }

            t.setTenants(tenants);

            return t;
        }
    }

    static class TenantBuilder {
        private com.latticeengines.security.globalauth.generated.service.Tenant tenant;

        public TenantBuilder(com.latticeengines.security.globalauth.generated.service.Tenant tenant) {
            this.tenant = tenant;
        }

        public Tenant build() {
            Tenant t = new Tenant();
            t.setName(tenant.getDisplayName().getValue());
            t.setId(tenant.getIdentifier().getValue());
            return t;
        }
    }

    private static long convertTicksToMilliseconds(long ticks) {
        long januaryFirst1970 = 621355968000000000L;
        long milliSecTo100NanoSec = 10000L;
        return (ticks - januaryFirst1970) / milliSecTo100NanoSec;
    }

}
