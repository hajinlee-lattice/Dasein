package com.latticeengines.testframework.exposed.proxy;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("adminLoginProxy")
public class AdminLoginProxy extends BaseRestApiProxy {

    private static final Logger log = LoggerFactory.getLogger(AdminLoginProxy.class);

    public AdminLoginProxy() {
        super(PropertyUtils.getProperty("common.test.admin.url"), "admin");
        setMaxAttempts(3);
    }

    protected String login(String username, String password) {
        String url = constructUrl("adlogin");
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(password);
        JsonNode json = post("adlogin", url, creds, JsonNode.class, false);
        if (json == null) {
            throw new RuntimeException("Failed to login AD for the user " + username); // don't put pw in this log
        }
        String token = json.get("Token").asText();
        if (StringUtils.isBlank(token)) {
            throw new RuntimeException("Failed to login AD for the user " + username); // don't put pw in this log
        } else {
            log.info("Successfully logged in the AD user " + username); // don't put pw in this log
        }
        return token;
    }

}
