package com.latticeengines.security.functionalframework;

import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GlobalAuthAccessResource {

    @RequestMapping(value = "/garesource/principal", method = RequestMethod.GET, headers = "Accept=application/json")
    public String getPrincipal() {
        return SecurityContextHolder.getContext().getAuthentication().getPrincipal().toString();
    }

    @RequestMapping(value = "/garesource/bad", method = RequestMethod.GET, headers = "Accept=application/json")
    public String getBad() {
        throw new RuntimeException("I want to fail");
    }

}
