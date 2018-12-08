package com.latticeengines.eai.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.network.exposed.eai.ValidateCredentialInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "validateCredential", description = "REST resource for importing data into Lattice")
@RestController
@RequestMapping("/validatecredential/customerspaces/{customerSpace}")
public class ValidateCredentialResource implements ValidateCredentialInterface {

    @Inject
    private ValidateCredentialResourceHelper validateCredentialResourceHelper;

    @RequestMapping(value = "/sourcetypes/{sourceType}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "validate credential")
    public SimpleBooleanResponse validateCredential(@PathVariable String customerSpace, @PathVariable String sourceType,
            @RequestBody CrmCredential crmCredential) {
        return validateCredentialResourceHelper.validateSourceCredential(customerSpace, sourceType, crmCredential);
    }
}
