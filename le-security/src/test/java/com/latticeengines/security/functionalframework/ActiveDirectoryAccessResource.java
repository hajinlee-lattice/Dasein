package com.latticeengines.security.functionalframework;

import java.util.HashMap;
import java.util.Map;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ActiveDirectoryAccessResource {

    @PreAuthorize("hasRole('Platform Operations')")
    @RequestMapping(value = "/adresource/hasaccess", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    public Map<String, String> getSomethingWithAccess() {
        Map<String, String> returnVal = new HashMap<>();
        returnVal.put("SomeReturnValue", "ABCD");
        return returnVal;
    }
    
    @PreAuthorize("hasRole('Enterprise Admins1')")
    @RequestMapping(value = "/adresource/noaccess", method = RequestMethod.GET, headers = "Accept=application/json")
    public Map<String, String> getSomethingWithoutAccess() {
        Map<String, String> returnVal = new HashMap<>();
        returnVal.put("SomeReturnValue", "ABCD");
        return returnVal;
    }
    
    
}
