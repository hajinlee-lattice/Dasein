package com.latticeengines.eai.controller;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.eai.exposed.service.EaiCredentialValidationService;

@Component("validateCredentialResourceHelper")
public class ValidateCredentialResourceHelper {

    private Logger log = LoggerFactory.getLogger(ValidateCredentialResourceHelper.class);

    @Inject
    private EaiCredentialValidationService eaiCredentialValidationService;

    public SimpleBooleanResponse validateSourceCredential(@PathVariable String customerSpace, //
            @PathVariable String sourceType, @RequestBody CrmCredential crmCredential) {
        try {
            crmCredential.setPassword(CipherUtils.decrypt(crmCredential.getPassword()));
            eaiCredentialValidationService.validateSourceCredential(customerSpace, sourceType, crmCredential);
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            List<String> error = Collections.singletonList(e.getMessage());
            return SimpleBooleanResponse.failedResponse(error);
        }
        return SimpleBooleanResponse.successResponse();
    }
}
