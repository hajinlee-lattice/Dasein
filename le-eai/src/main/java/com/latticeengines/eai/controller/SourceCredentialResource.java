package com.latticeengines.eai.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "validateCredential", description = "REST resource for importing data into Lattice")
@RestController
@RequestMapping("/validatecredential/customerspaces/{customerSpace}")
public class SourceCredentialResource {

    @Autowired
    private SourceCredentialResourceHelper sourceCredentialResourceHelper;

    @RequestMapping(value = "/sourcetypes/{sourceType}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create an import data job")
    public SimpleBooleanResponse verifySourceCredential(@PathVariable String customerSpace,
            @PathVariable String sourceType, @RequestBody CrmCredential crmCredential) {
        return sourceCredentialResourceHelper.validateSourceCredential(customerSpace, sourceType, crmCredential);
    }
}
