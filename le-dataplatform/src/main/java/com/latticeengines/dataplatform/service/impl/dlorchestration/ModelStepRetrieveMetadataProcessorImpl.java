package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.google.common.base.Strings;
import com.latticeengines.common.exposed.util.HttpUtils;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepProcessor;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;

@Component("modelStepRetrieveMetadataProcessor")
public class ModelStepRetrieveMetadataProcessorImpl implements ModelStepProcessor {

    private static final String DL_CONFIG_SERVICE_GET_QUERY_META_DATA_COLUMNS = "/DLConfigService/GetQueryMetaDataColumns";
    private static final String DL_CONFIG_SERVICE_LOGIN = "/DLConfigService/Login";

    // Make this settable for easier testing
    private String loginUrlSuffix = DL_CONFIG_SERVICE_LOGIN;
    private String queryMetadataUrlSuffix = DL_CONFIG_SERVICE_GET_QUERY_META_DATA_COLUMNS;

    public void setLoginUrlSuffix(String loginUrlSuffix) {
        this.loginUrlSuffix = loginUrlSuffix;
    }

    public void setQueryMetadataUrlSuffix(String queryMetadataUrlSuffix) {
        this.queryMetadataUrlSuffix = queryMetadataUrlSuffix;
    }

    @Override
    public void executeStep(ModelCommand modelCommand, ModelCommandParameters modelCommandParameters) {
        String loginUrl = modelCommandParameters.getDlUrl() + loginUrlSuffix;
        String queryMetadataUrl = modelCommandParameters.getDlUrl() + queryMetadataUrlSuffix;

        String sessionId = null;
        try {
            Map<String, String> parameters = new HashMap<>();
            parameters.put("userName", modelCommandParameters.getDlUsername());
            parameters.put("password", modelCommandParameters.getDlPassword());
            parameters.put("token", modelCommandParameters.getDlToken());
            sessionId = HttpUtils.executePostRequest(loginUrl, parameters);
            if (Strings.isNullOrEmpty(sessionId)) {
                throw new LedpException(LedpCode.LEDP_16004, new String[] { String.valueOf(modelCommand.getPid()), loginUrl });
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_16003, e, new String[] { String.valueOf(modelCommand.getPid()), loginUrl });
        }

        String metadata = null;
        try {
            Map<String, String> parameters = new HashMap<>();
            parameters.put("sessionId", sessionId);
            parameters.put("tenantName", modelCommandParameters.getDlTenant());
            parameters.put("queryName", modelCommandParameters.getEventTable());
            metadata = HttpUtils.executePostRequest(queryMetadataUrl, parameters);
            if (Strings.isNullOrEmpty(metadata)) {
                throw new LedpException(LedpCode.LEDP_16006, new String[] { String.valueOf(modelCommand.getPid()), queryMetadataUrl });
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_16005, e, new String[] { String.valueOf(modelCommand.getPid()), queryMetadataUrl });
        }

        // TODO do something with metadata
    }

}

