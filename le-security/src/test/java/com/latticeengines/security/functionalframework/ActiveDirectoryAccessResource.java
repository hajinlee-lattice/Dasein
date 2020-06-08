package com.latticeengines.security.functionalframework;

import java.util.HashMap;
import java.util.Map;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ActiveDirectoryAccessResource {

    @PreAuthorize("hasRole('adminconsole')")
    @GetMapping("/adresource/hasaccess")
    @ResponseBody
    public Map<String, String> getSomethingWithAccess() {
        Map<String, String> returnVal = new HashMap<>();
        returnVal.put("SomeReturnValue", "ABCD");
        return returnVal;
    }

    @PreAuthorize("hasRole('Enterprise Admins1')")
    @GetMapping("/adresource/noaccess")
    public Map<String, String> getSomethingWithoutAccess() {
        Map<String, String> returnVal = new HashMap<>();
        returnVal.put("SomeReturnValue", "ABCD");
        return returnVal;
    }

}
