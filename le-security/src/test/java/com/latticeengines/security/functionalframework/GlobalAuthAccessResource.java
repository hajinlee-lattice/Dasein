package com.latticeengines.security.functionalframework;

import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GlobalAuthAccessResource {

    @GetMapping("/garesource/principal")
    public String getPrincipal() {
        return SecurityContextHolder.getContext().getAuthentication().getPrincipal().toString();
    }

    @GetMapping("/garesource/bad")
    public String getBad() {
        throw new RuntimeException("I want to fail");
    }

}
