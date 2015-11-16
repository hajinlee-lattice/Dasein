package com.latticeengines.eai.controller;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.eai.exposed.service.EaiCredentialValidationService;

@Component("sourceCredentialResourceHelper")
public class SourceCredentialResourceHelper {

    private Logger log = Logger.getLogger(SourceCredentialResourceHelper.class);

    @Autowired
    private EaiCredentialValidationService eaiCredentialValidationService;

    public SimpleBooleanResponse validateSourceCredential(@PathVariable String customerSpace, //
            @PathVariable String sourceType, @RequestBody CrmCredential crmCredential) {
        try {
            crmCredential.setPassword(CipherUtils.decrypt(crmCredential.getPassword()));
            eaiCredentialValidationService.validateSourceCredential(customerSpace, sourceType, crmCredential);
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            List<String> error = Arrays.<String> asList(new String[] { e.getMessage() });
            return SimpleBooleanResponse.failedResponse(error);
        }
        return SimpleBooleanResponse.successResponse();
    }
}
