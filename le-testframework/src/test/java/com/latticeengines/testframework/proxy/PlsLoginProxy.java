package com.latticeengines.testframework.proxy;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("globalAuthLoginProxy")
public class PlsLoginProxy extends BaseRestApiProxy {

    private static final Log log = LogFactory.getLog(AdminLoginProxy.class);

    public PlsLoginProxy() {
        super(PropertyUtils.getProperty("common.test.pls.url"), "pls");
        setMaxAttempts(3);
    }

    protected String login(String username, String password) {
        String url = constructUrl("login");
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(password);
        LoginDocument doc = post("login pls", url, creds, LoginDocument.class, false);
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
