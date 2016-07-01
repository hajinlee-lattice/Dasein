package com.latticeengines.saml;

import java.util.List;

import javax.annotation.Nullable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.saml.SAMLCredential;
import org.springframework.security.saml.userdetails.SAMLUserDetailsService;
import org.springframework.stereotype.Component;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.saml.entitymgr.IdentityProviderEntityMgr;
import com.latticeengines.saml.util.SAMLUtils;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.UserService;

@Component("latticeUserDetailsService")
public class LatticeUserDetailsService implements SAMLUserDetailsService {

    @Autowired
    private IdentityProviderEntityMgr identityProviderEntityMgr;

    @Autowired
    private UserService userService;

    @Override
    public Object loadUserBySAML(final SAMLCredential samlCredential) throws UsernameNotFoundException {
        String tenantId = SAMLUtils.getTenantIdFromLocalEntityId(samlCredential.getLocalEntityID());
        if (tenantId == null) {
            throw new RuntimeException(String.format("Invalid local entity Id: %s", samlCredential.getLocalEntityID()));
        }

        List<IdentityProvider> identityProviders = identityProviderEntityMgr.findByTenantId(tenantId);
        IdentityProvider match = Iterables.find(identityProviders, new Predicate<IdentityProvider>() {
            @Override
            public boolean apply(@Nullable IdentityProvider identityProvider) {
                return samlCredential.getRemoteEntityID().equals(identityProvider.getEntityId());
            }
        }, null);
        if (match == null) {
            throw new UsernameNotFoundException(String.format("IdP %s cannot be used",
                    samlCredential.getRemoteEntityID()));
        }

        User user = userService.findByEmail(samlCredential.getNameID().getValue());
        if (user == null) {
            throw new UsernameNotFoundException(String.format("User with email %s was not found", samlCredential
                    .getNameID().getValue()));
        }

        if (!user.isActive()) {
            throw new UsernameNotFoundException(String.format("User with email %s is not active", samlCredential
                    .getNameID().getValue()));
        }

        AccessLevel level = userService.getAccessLevel(tenantId, user.getUsername());
        if (level == null) {
            throw new UsernameNotFoundException("Unauthorized");
        }

        return user;
    }
}
