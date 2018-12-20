package com.latticeengines.security.exposed.globalauth.impl;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public abstract class GlobalAuthenticationServiceBaseImpl {

    @Value("${security.globalauth.url}")
    protected String globalAuthUrl;

}
