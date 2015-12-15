package com.latticeengines.security.exposed.globalauth.impl;

import java.net.URL;

import javax.xml.bind.JAXBElement;
import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.monitor.annotation.RestApiCall;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.globalauth.GlobalSessionManagementService;
import com.latticeengines.security.globalauth.generated.sessionmgr.ISessionManagementService;
import com.latticeengines.security.globalauth.generated.sessionmgr.ObjectFactory;
import com.latticeengines.security.globalauth.generated.sessionmgr.SessionManagementService;

@Component("globalSessionManagementService")
public class GlobalSessionManagementServiceImpl
        extends GlobalAuthenticationServiceBaseImpl
        implements GlobalSessionManagementService {

    private static final Log LOGGER = LogFactory.getLog(GlobalSessionManagementServiceImpl.class);

    @RestApiCall
    private ISessionManagementService getService() {
        SessionManagementService service;
        try {
            service = new SessionManagementService(new URL(globalAuthUrl + "/GlobalAuthSessionManager?wsdl"));
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18000, e, new String[]{globalAuthUrl});
        }
        return service.getBasicHttpBindingISessionManagementService();
    }

    @Override
    @RestApiCall
    public synchronized Session retrieve(Ticket ticket) {
        if (ticket == null) {
            throw new NullPointerException("Ticket cannot be null.");
        }
        if (ticket.getRandomness() == null) {
            throw new NullPointerException("Ticket.getRandomness() cannot be null.");
        }
        if (ticket.getUniqueness() == null) {
            throw new NullPointerException("Ticket.getUniqueness() cannot be null.");
        }
        ISessionManagementService service = getService();
        addMagicHeaderAndSystemProperty(service);
        try {
            LOGGER.info(String.format("Retrieving session from ticket %s against Global Auth.", ticket.toString()));
            Session s = new SessionBuilder(service.retrieve(new SoapTicketBuilder(ticket).build())).build();
            s.setTicket(ticket);
            return s;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18002, e, new String[]{ticket.getData()});
        }
    }

    @Override
    @RestApiCall
    public synchronized Session attach(Ticket ticket) {
        if (ticket == null) {
            throw new NullPointerException("Ticket cannot be null.");
        }
        if (ticket.getRandomness() == null) {
            throw new NullPointerException("Ticket.getRandomness() cannot be null.");
        }
        if (ticket.getUniqueness() == null) {
            throw new NullPointerException("Ticket.getUniqueness() cannot be null.");
        }
        if (ticket.getTenants().size() == 0) {
            throw new RuntimeException("There must be at least one tenant in the ticket.");
        }
        ISessionManagementService service = getService();
        addMagicHeaderAndSystemProperty(service);
        try {
            LOGGER.info(String.format("Attaching ticket %s against Global Auth.", ticket.toString()));

            Session s = new SessionBuilder(service.attach(
                    new SoapTicketBuilder(ticket).build(),
                    new SoapTenantBuilder(ticket.getTenants().get(0)).build())).build();
            s.setTicket(ticket);
            return s;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18001, e, new String[]{ticket.toString()});
        }
    }

    static class SoapTicketBuilder {
        private Ticket ticket;

        public SoapTicketBuilder(Ticket ticket) {
            this.ticket = ticket;
        }

        public com.latticeengines.security.globalauth.generated.sessionmgr.Ticket build() {
            com.latticeengines.security.globalauth.generated.sessionmgr.Ticket t = new ObjectFactory().createTicket();
            t.setUniquness(ticket.getUniqueness());
            t.setRandomness(new JAXBElement<String>(
                    new QName("http://schemas.lattice-engines.com/2008/Poet", "Randomness"), //
                    String.class, ticket.getRandomness()));
            return t;
        }
    }

    static class SoapTenantBuilder {
        private Tenant tenant;

        public SoapTenantBuilder(Tenant tenant) {
            this.tenant = tenant;
        }

        public com.latticeengines.security.globalauth.generated.sessionmgr.Tenant build() {
            com.latticeengines.security.globalauth.generated.sessionmgr.Tenant t = new ObjectFactory().createTenant();
            t.setIdentifier(new JAXBElement<String>(
                    new QName("http://schemas.lattice-engines.com/2008/Poet", "Identifier"), //
                    String.class, tenant.getId()));
            t.setDisplayName(new JAXBElement<String>(
                    new QName("http://schemas.lattice-engines.com/2008/Poet", "DisplayName"), //
                    String.class, tenant.getName()));
            return t;
        }
    }

    static class SessionBuilder {
        private com.latticeengines.security.globalauth.generated.sessionmgr.Session session;

        public SessionBuilder(com.latticeengines.security.globalauth.generated.sessionmgr.Session session) {
            this.session = session;
        }

        public Session build() {
            Session s = new Session();
            s.setTenant(new TenantBuilder(session.getTenant()).build());
            s.setRights(session.getRights().getValue().getString());
            s.setDisplayName(session.getDisplayName().getValue());
            s.setEmailAddress(session.getEmailAddress().getValue());
            s.setIdentifier(session.getIdentifier().getValue());
            s.setLocale(session.getLocale().getValue());
            s.setTitle(session.getTitle().getValue());
            return s;
        }
    }

    static class TenantBuilder {
        private com.latticeengines.security.globalauth.generated.sessionmgr.Tenant tenant;

        public TenantBuilder(com.latticeengines.security.globalauth.generated.sessionmgr.Tenant tenant) {
            this.tenant = tenant;
        }

        public Tenant build() {
            Tenant t = new Tenant();
            t.setName(tenant.getDisplayName().getValue());
            t.setId(tenant.getIdentifier().getValue());
            return t;
        }
    }

}
