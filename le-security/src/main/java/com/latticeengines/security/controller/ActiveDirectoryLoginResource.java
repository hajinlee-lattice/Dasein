package com.latticeengines.security.controller;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.ldap.authentication.ad.ActiveDirectoryLdapAuthenticationProvider;
import org.springframework.security.ldap.userdetails.LdapUserDetailsImpl;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.domain.exposed.security.Credentials;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "adlogin", description = "REST resource for logging in using Active Directory")
@RestController
@RequestMapping("/adlogin")
public class ActiveDirectoryLoginResource {

    @Value("${common.le.environment}")
    private String leEnv;

    private static final Logger log = LoggerFactory.getLogger(ActiveDirectoryLoginResource.class);

    private final ActiveDirectoryLdapAuthenticationProvider activeDirectoryProvider;

    public ActiveDirectoryLoginResource(@Value("${security.ldap.domain}") String domain,
            @Value("${security.ldap.url}") String url) {
        activeDirectoryProvider = new ActiveDirectoryLdapAuthenticationProvider(domain, url);
        activeDirectoryProvider.setConvertSubErrorCodesToExceptions(true);
        activeDirectoryProvider.setUseAuthenticationRequestCredentials(true);
    }

    @PostMapping("")
    @ApiOperation(value = "Login using ActiveDirectory")
    public ADLoginDocument login(@RequestBody Credentials creds) {
        try {
            String username = creds.getUsername();
            String password = creds.getPassword();
            if (username == null) {
                username = "";
            }
            if (password == null) {
                password = "";
            }
            UsernamePasswordAuthenticationToken authRequest = new UsernamePasswordAuthenticationToken(username.trim(),
                    password);

            UsernamePasswordAuthenticationToken auth;

            if ("dev".equals(leEnv) && "testuser1".equals(username)) {
                auth = new UsernamePasswordAuthenticationToken("testuser1", "Lattice1", Collections.singletonList(new SimpleGrantedAuthority("adminconsole")));
                return buildFakeToken(auth);
            } else {
                auth = (UsernamePasswordAuthenticationToken) activeDirectoryProvider
                        .authenticate(authRequest);
                return buildToken(auth);
            }

        } catch (Exception e) {
            log.error("Bad AD Login", e);
            return null;
        }
    }

    private static class ADLoginDocument {
        @JsonProperty("Token")
        private String token;

        @JsonProperty("Principal")
        private String principal;

        @JsonProperty("Roles")
        private List<String> roles;
    }

    private ADLoginDocument buildToken(UsernamePasswordAuthenticationToken auth) {
        LdapUserDetailsImpl ldapDetails = (LdapUserDetailsImpl) auth.getPrincipal();
        StringBuilder token = new StringBuilder(ldapDetails.getUsername());
        token.append("|").append(System.currentTimeMillis()).append("|");
        Collection<? extends GrantedAuthority> rights = auth.getAuthorities();
        token.append(StringUtils.join(rights, "|"));
        String encrypted = CipherUtils.encrypt(token.toString());
        encrypted = encrypted.replaceAll("[\\r\\n\\t]+", "");
        ADLoginDocument loginDocument = new ADLoginDocument();
        loginDocument.token = encrypted;
        loginDocument.principal = ldapDetails.getUsername();
        loginDocument.roles = rights.stream().map(GrantedAuthority::getAuthority).collect(Collectors.toList());
        return loginDocument;
    }

    private ADLoginDocument buildFakeToken(UsernamePasswordAuthenticationToken auth) {
        StringBuilder token = new StringBuilder("testuser1");
        token.append("|").append(System.currentTimeMillis()).append("|");
        Collection<? extends GrantedAuthority> rights = auth.getAuthorities();
        token.append(StringUtils.join(rights, "|"));
        String encrypted = CipherUtils.encrypt(token.toString());
        encrypted = encrypted.replaceAll("[\\r\\n\\t]+", "");
        ADLoginDocument loginDocument = new ADLoginDocument();
        loginDocument.token = encrypted;
        loginDocument.principal = "testuser1";
        loginDocument.roles = rights.stream().map(GrantedAuthority::getAuthority).collect(Collectors.toList());
        return loginDocument;
    }

}
