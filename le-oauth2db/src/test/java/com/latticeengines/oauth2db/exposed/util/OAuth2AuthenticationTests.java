package com.latticeengines.oauth2db.exposed.util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.provider.AuthorizationRequest;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.OAuth2Request;
import org.springframework.security.oauth2.provider.authentication.OAuth2AuthenticationDetails;
import org.springframework.test.annotation.Rollback;
import org.springframework.util.SerializationUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class OAuth2AuthenticationTests {

    private OAuth2Request request = RequestTokenFactory.createOAuth2Request(null, "id", null, false,
            Collections.singleton("read"), null, null, null, null);

    private UsernamePasswordAuthenticationToken userAuthentication = new UsernamePasswordAuthenticationToken(
            "foo", "bar", Collections.singleton(new SimpleGrantedAuthority("ROLE_USER")));

    @Test(groups = "unit")
    @Rollback
    public void testIsAuthenticated() {
        request = RequestTokenFactory.createOAuth2Request("id", true,
                Collections.singleton("read"));
        OAuth2Authentication authentication = new OAuth2Authentication(request, userAuthentication);
        Assert.assertTrue(authentication.isAuthenticated());
    }

    @Test(groups = "unit")
    public void testGetCredentials() {
        OAuth2Authentication authentication = new OAuth2Authentication(request, userAuthentication);
        Assert.assertEquals("", authentication.getCredentials());
    }

    @Test(groups = "unit")
    public void testGetPrincipal() {
        OAuth2Authentication authentication = new OAuth2Authentication(request, userAuthentication);
        Assert.assertEquals(userAuthentication.getPrincipal(), authentication.getPrincipal());
    }

    @Test(groups = "unit")
    public void testIsClientOnly() {
        OAuth2Authentication authentication = new OAuth2Authentication(request, null);
        Assert.assertTrue(authentication.isClientOnly());
    }

    @Test(groups = "unit")
    public void testJsonSerialization() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(OAuth2Authentication.class, new OAuth2AuthenticationDeserializer());
        mapper.registerModule(module);

        OAuth2Authentication holder = new OAuth2Authentication(request, userAuthentication);
        String json = mapper.writeValueAsString(holder);

        OAuth2Authentication other = mapper.readValue(json, OAuth2Authentication.class);
        Assert.assertEquals(holder, other);
    }

    @Test(groups = "unit")
    public void testSerialization() {
        OAuth2Authentication holder = new OAuth2Authentication(
                new AuthorizationRequest("client", Arrays.asList("read")).createOAuth2Request(),
                new UsernamePasswordAuthenticationToken("user", "pwd"));
        OAuth2Authentication other = (OAuth2Authentication) SerializationUtils
                .deserialize(SerializationUtils.serialize(holder));
        Assert.assertEquals(holder, other);
    }

    @Test(groups = "unit")
    public void testSerializationWithDetails() {
        OAuth2Authentication holder = new OAuth2Authentication(
                new AuthorizationRequest("client", Arrays.asList("read")).createOAuth2Request(),
                new UsernamePasswordAuthenticationToken("user", "pwd"));
        holder.setDetails(new OAuth2AuthenticationDetails(new MockHttpServletRequest()));
        OAuth2Authentication other = (OAuth2Authentication) SerializationUtils
                .deserialize(SerializationUtils.serialize(holder));
        Assert.assertEquals(holder, other);
    }

    @Test(groups = "unit")
    public void testEraseCredentialsUserAuthentication() {
        OAuth2Authentication authentication = new OAuth2Authentication(request, userAuthentication);
        authentication.eraseCredentials();
        Assert.assertNull(authentication.getUserAuthentication().getCredentials());
    }

    static class RequestTokenFactory {

        public static OAuth2Request createOAuth2Request(Map<String, String> requestParameters,
                String clientId, Collection<? extends GrantedAuthority> authorities,
                boolean approved, Collection<String> scope, Set<String> resourceIds,
                String redirectUri, Set<String> responseTypes,
                Map<String, Serializable> extensionProperties) {
            return new OAuth2Request(requestParameters, clientId, authorities, approved,
                    scope == null ? null : new LinkedHashSet<String>(scope), resourceIds,
                    redirectUri, responseTypes, extensionProperties);
        }

        public static OAuth2Request createOAuth2Request(String clientId, boolean approved) {
            return createOAuth2Request(clientId, approved, null);
        }

        public static OAuth2Request createOAuth2Request(String clientId, boolean approved,
                Collection<String> scope) {
            return createOAuth2Request(Collections.<String, String> emptyMap(), clientId, approved,
                    scope);
        }

        public static OAuth2Request createOAuth2Request(Map<String, String> parameters,
                String clientId, boolean approved, Collection<String> scope) {
            return createOAuth2Request(parameters, clientId, null, approved, scope, null, null,
                    null, null);
        }

    }
}
