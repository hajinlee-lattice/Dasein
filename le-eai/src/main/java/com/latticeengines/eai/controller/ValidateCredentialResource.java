package com.latticeengines.eai.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.network.exposed.eai.ValidateCredentialInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "validateCredential")
@RestController
@RequestMapping("/validatecredential/customerspaces/{customerSpace}")
public class ValidateCredentialResource implements ValidateCredentialInterface {

    @Inject
    private ValidateCredentialResourceHelper validateCredentialResourceHelper;

    @PostMapping("/sourcetypes/{sourceType}")
    @ResponseBody
    @ApiOperation(value = "validate credential")
    public SimpleBooleanResponse validateCredential(@PathVariable String customerSpace, @PathVariable String sourceType,
            @RequestBody CrmCredential crmCredential) {
        return validateCredentialResourceHelper.validateSourceCredential(customerSpace, sourceType, crmCredential);
    }
}
