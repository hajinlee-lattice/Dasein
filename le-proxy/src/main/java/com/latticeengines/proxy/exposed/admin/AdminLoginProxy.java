package com.latticeengines.proxy.exposed.admin;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("adminLoginProxy")
public class AdminLoginProxy extends BaseRestApiProxy {

    public AdminLoginProxy() {
        super(PropertyUtils.getProperty("common.admin.url"), "admin");
    }

    protected String login(String username, String password) {
        String url = constructUrl("adlogin");
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(password);
        JsonNode json = post("adlogin", url, creds, JsonNode.class, false);
        String token = json.get("Token").asText();
        if (StringUtils.isBlank(token)) {
            throw new RuntimeException("Failed to login AD for the user " + username); // don't put pw in this log
        }
        return token;
    }

}
