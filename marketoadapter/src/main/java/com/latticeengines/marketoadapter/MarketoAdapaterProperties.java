package com.latticeengines.marketoadapter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

@Configuration
@PropertySource("file:marketoAdapter.properties")
public class MarketoAdapaterProperties {
    public String getCamilleAddress() {
        return env.getProperty("marketoadapter.camille.address");
    }
    
    public String getSkaldAddress() {
        return env.getProperty("marketoadapter.skald.address");
    }
    
    @Autowired
    private Environment env;
}
