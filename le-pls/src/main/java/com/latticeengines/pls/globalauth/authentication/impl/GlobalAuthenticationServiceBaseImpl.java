package com.latticeengines.pls.globalauth.authentication.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.ws.BindingProvider;
import javax.xml.ws.handler.MessageContext;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public abstract class GlobalAuthenticationServiceBaseImpl {

    @Value("${pls.globalauth.url}")
    protected String globalAuthUrl;
    
    void addMagicHeaderAndSystemProperty(Object servicePort) {
        if (!(servicePort instanceof BindingProvider)) {
            throw new RuntimeException("Service is not of type BindingProvider.");
        }
        System.setProperty("javax.xml.bind.JAXBContext", "com.sun.xml.internal.bind.v2.ContextFactory");
        BindingProvider bp = (BindingProvider) servicePort;
        Map<String, List<String>> requestHeaders = new HashMap<>();
        requestHeaders.put("MagicAuthentication", Arrays.<String> asList("Security through obscurity!"));
        bp.getRequestContext().put(MessageContext.HTTP_REQUEST_HEADERS, requestHeaders);
    }

}
