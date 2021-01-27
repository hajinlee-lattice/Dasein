package com.latticeengines.security.provider.activedirectory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.engine.jdbc.StreamUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.ldap.userdetails.LdapUserDetailsImpl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.provider.AbstractAuthenticationTokenFilter;

public class RestActiveDirectoryFilter extends AbstractAuthenticationTokenFilter {

    @Value("${common.le.environment}")
    private String leEnv;

    @Override
    public Authentication attemptAuthentication(HttpServletRequest request, HttpServletResponse response)
            throws AuthenticationException {
        String ticket = request.getHeader(Constants.AUTHORIZATION);
        String username = null;
        String password = null;

        if (ticket == null) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                StreamUtils.copy(request.getInputStream(), baos);
            } catch (IOException e) {
                throw new AuthenticationServiceException(e.getMessage(), e);
            }
            String body = new String(baos.toByteArray());
            Credentials creds = JsonUtils.deserialize(body, Credentials.class);
            username = creds.getUsername();
            password = creds.getPassword();

            if (username == null) {
                username = "";
            }

            if (password == null) {
                password = "";
            }

            UsernamePasswordAuthenticationToken authRequest = new UsernamePasswordAuthenticationToken(username.trim(),
                    password);

            UsernamePasswordAuthenticationToken auth;

            String token;
            if ("dev".equals(leEnv) && "testuser1".equals(username)) {
                auth = new UsernamePasswordAuthenticationToken("testuser1", "Lattice1", Collections.singletonList(new SimpleGrantedAuthority("adminconsole")));
                token = buildFakeToken(auth);
            } else {
                auth = (UsernamePasswordAuthenticationToken) getAuthenticationManager()
                        .authenticate(authRequest);
                token = buildToken(auth);
            }

            try {
                response.setContentType("application/json; charset=UTF-8");
                response.getOutputStream().write(token.getBytes());
                response.getOutputStream().flush();
            } catch (Exception e) {
                throw new BadCredentialsException("Unauthorized.", e);
            }
            return auth;
        } else {
            try {
                return buildAuth(ticket);
            } catch (Exception e) {
                throw new BadCredentialsException("Unauthorized.", e);
            }
        }
    }

    private String buildToken(UsernamePasswordAuthenticationToken auth){
        LdapUserDetailsImpl ldapDetails = (LdapUserDetailsImpl) auth.getPrincipal();
        StringBuilder token = new StringBuilder(ldapDetails.getUsername());
        token.append("|").append(System.currentTimeMillis()).append("|");
        Collection<? extends GrantedAuthority> rights = auth.getAuthorities();
        token.append(StringUtils.join(rights, "|"));
        String encrypted = CipherUtils.encrypt(token.toString());
        encrypted = encrypted.replaceAll("[\\r\\n\\t]+", "");
        ObjectNode oNode = new ObjectMapper().createObjectNode();
        oNode.put("Token", encrypted);
        oNode.put("Principal", ldapDetails.getUsername());
        oNode.putArray("Roles");
        ArrayNode aNode = (ArrayNode) oNode.get("Roles");
        for (GrantedAuthority right : rights) {
            aNode.add(removeRolePrefix(right.getAuthority()));
        }
        return oNode.toString();
    }

    private String buildFakeToken(UsernamePasswordAuthenticationToken auth) {
        StringBuilder token = new StringBuilder("testuser1");
        token.append("|").append(System.currentTimeMillis()).append("|");
        Collection<? extends GrantedAuthority> rights = auth.getAuthorities();
        token.append(StringUtils.join(rights, "|"));
        String encrypted = CipherUtils.encrypt(token.toString());
        encrypted = encrypted.replaceAll("[\\r\\n\\t]+", "");
        ObjectNode oNode = new ObjectMapper().createObjectNode();
        oNode.put("Token", encrypted);
        oNode.put("Principal", "testuser1");
        oNode.putArray("Roles");
        ArrayNode aNode = (ArrayNode) oNode.get("Roles");
        for (GrantedAuthority right : rights) {
            aNode.add(removeRolePrefix(right.getAuthority()));
        }
        return oNode.toString();
    }



    private UsernamePasswordAuthenticationToken buildAuth(String ticket) throws Exception {
        String decrypted = CipherUtils.decrypt(ticket);
        String[] tokens = decrypted.split("\\|");

        long ticketTime = Long.parseLong(tokens[1]);

        if (!isSameDay(ticketTime, System.currentTimeMillis())) {
            throw new BadCredentialsException("Token expired.");
        }
        List<GrantedAuthority> rights = new ArrayList<>();
        for (int i = 2; i < tokens.length; i++) {
            rights.add(new SimpleGrantedAuthority(addRolePrefix(tokens[i])));
        }
        return new UsernamePasswordAuthenticationToken(tokens[0], null, rights);
    }

    boolean isSameDay(long ticketTime, long currentTime) {
        Calendar ticketDay = Calendar.getInstance();
        ticketDay.setTimeInMillis(ticketTime);

        Calendar today = Calendar.getInstance();
        today.setTimeInMillis(currentTime);

        return ticketDay.get(Calendar.DATE) == today.get(Calendar.DATE);
    }

}
