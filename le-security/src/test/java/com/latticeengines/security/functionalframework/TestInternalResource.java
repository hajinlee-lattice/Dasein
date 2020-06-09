package com.latticeengines.security.functionalframework;

import java.util.HashMap;
import java.util.Map;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.StatusDocument;

@RestController
public class TestInternalResource {

    @GetMapping("/internal/resource")
    @ResponseBody
    public Map<String, String> getSomeInternalResource() {
        Map<String, String> returnVal = new HashMap<>();
        returnVal.put("SomeInternalValue", "ABCD");
        return returnVal;
    }

    @GetMapping("/internal/health")
    @ResponseBody
    public StatusDocument checkHealth() {
        return StatusDocument.online();
    }

    @GetMapping("/internal/bad")
    public String getBad() {
        throw new RuntimeException("I want to fail");
    }

    @GetMapping("/internal/noaccess")
    public String getNoAccess() {
        return "You should not see this!";
    }

}
