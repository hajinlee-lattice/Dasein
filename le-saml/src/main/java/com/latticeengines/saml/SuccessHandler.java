package com.latticeengines.saml;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.saml.SAMLCredential;
import org.springframework.security.web.authentication.AuthenticationSuccessHandler;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.saml.LoginValidationResponse;
import com.latticeengines.saml.util.SAMLUtils;

public class SuccessHandler implements AuthenticationSuccessHandler {

    private static final Logger log = LoggerFactory.getLogger(SuccessHandler.class);

    private static final String LATTICE_ROLES = "lattice.roles";
    private static final String LATTICE_NAME_LAST = "lattice.name.last";
    private static final String LATTICE_NAME_FIRST = "lattice.name.first";

    @Override
    public void onAuthenticationSuccess(HttpServletRequest request, HttpServletResponse response,
            Authentication authentication) throws ServletException, IOException {
        log.info(String.format("SAML Authentication successful for user %s", authentication.getName()));

        LoginValidationResponse resp = new LoginValidationResponse();
        SAMLCredential cred = (SAMLCredential) authentication.getCredentials();

        String email = authentication.getName();
        String firstName = cred.getAttributeAsString(LATTICE_NAME_FIRST);
        String lastName = cred.getAttributeAsString(LATTICE_NAME_LAST);
        String rolestr = cred.getAttributeAsString(LATTICE_ROLES);
        List<String> userRoles = null;
        if (StringUtils.isNotBlank(rolestr)) {
            rolestr = rolestr.trim();
            userRoles = Arrays.asList(rolestr.split(",")) //
                    .stream() //
                    .filter(role -> StringUtils.isNotBlank(role)) //
                    .map(role -> role.trim()) //
                    .collect(Collectors.toList());
        }

        String tenantId = SAMLUtils.getTenantFromAlias(request.getPathInfo());

        log.info(String.format(
                "request.getPathInfo() = %s, email = %s, tenantId = %s, first name = %s, last name = %s, roles = %s",
                request.getPathInfo(), email, tenantId, firstName, lastName, rolestr));

        try (ServletOutputStream os = response.getOutputStream()) {
            response.setContentType(MediaType.APPLICATION_JSON);
            response.setStatus(HttpStatus.SC_OK);
            resp.setValidated(true);
            resp.setUserId(email);
            resp.setFirstName(firstName);
            resp.setLastName(lastName);
            resp.setUserRoles(userRoles);
            JsonUtils.serialize(resp, os);
        }
    }

}
