package com.latticeengines.testframework.exposed.proxy.pls;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("globalAuthLoginProxy")
public class PlsLoginProxy extends BaseRestApiProxy {

    private static final Logger log = LoggerFactory.getLogger(PlsLoginProxy.class);

    public PlsLoginProxy() {
        super(PropertyUtils.getProperty("common.test.pls.url"), "pls");
    }

    protected String login(String username, String password) {
        String url = constructUrl("login");
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(password);
        LoginDocument doc = post("login pls", url, creds, LoginDocument.class);
        if (doc == null) {
            throw new RuntimeException("Failed to login GA for the user " + username); // don't put pw in this log
        }
        String token = doc.getData();
        if (StringUtils.isBlank(token)) {
            throw new RuntimeException("Failed to login GA for the user " + username); // don't put pw in this log
        } else {
            log.info("Successfully logged in the GA user " + username); // don't put pw in this log
        }
        return token;
    }

}
