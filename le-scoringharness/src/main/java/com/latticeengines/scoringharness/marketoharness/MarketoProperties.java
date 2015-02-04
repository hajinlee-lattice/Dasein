package com.latticeengines.scoringharness.marketoharness;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

@Configuration
@PropertySource("file:scoringharness.properties")
public class MarketoProperties {
    public String getClientId() {
        return env.getProperty("scoringharness.marketo.authentication.clientid").trim();
    }

    public String getClientSecret() {
        return env.getProperty("scoringharness.marketo.authentication.clientsecret").trim();
    }

    public String getScoreField() {
        return env.getProperty("scoringharness.marketo.scorefield").trim();
    }

    @Autowired
    private Environment env;
}
