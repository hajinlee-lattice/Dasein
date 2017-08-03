package com.latticeengines.security.functionalframework;

import java.util.HashMap;
import java.util.Map;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestInternalResource {

    @RequestMapping(value = "/internal/resource", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    public Map<String, String> getSomeInternalResource() {
        Map<String, String> returnVal = new HashMap<>();
        returnVal.put("SomeInternalValue", "ABCD");
        return returnVal;
    }

}
