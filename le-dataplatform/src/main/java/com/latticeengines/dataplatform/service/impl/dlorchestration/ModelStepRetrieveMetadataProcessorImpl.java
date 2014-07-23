package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.base.Strings;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HttpUtils;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepProcessor;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.visidb.GetQueryMetaDataColumnsRequest;

@Component("modelStepRetrieveMetadataProcessor")
public class ModelStepRetrieveMetadataProcessorImpl implements ModelStepProcessor {
    private static final Log log = LogFactory.getLog(ModelStepRetrieveMetadataProcessorImpl.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;
    
    private static final String DL_CONFIG_SERVICE_GET_QUERY_META_DATA_COLUMNS = "/GetQueryMetaDataColumns";

    // Make this settable for easier testing
    private String queryMetadataUrlSuffix = DL_CONFIG_SERVICE_GET_QUERY_META_DATA_COLUMNS;

    public void setQueryMetadataUrlSuffix(String queryMetadataUrlSuffix) {
        this.queryMetadataUrlSuffix = queryMetadataUrlSuffix;
    }

    @Override
    public void executeStep(ModelCommand modelCommand, ModelCommandParameters modelCommandParameters) {
        String queryMetadataUrl = modelCommandParameters.getDlUrl() + queryMetadataUrlSuffix;
        String metadata = null;
        try {
            GetQueryMetaDataColumnsRequest request = new GetQueryMetaDataColumnsRequest(modelCommandParameters.getDlTenant(),
                    modelCommandParameters.getEventTable());
            Map<String, String> headers = new HashMap<String, String>();
            headers.put("MagicAuthentication", "Security through obscurity!");
            metadata = HttpUtils.executePostRequest(queryMetadataUrl, request, headers);
            if (Strings.isNullOrEmpty(metadata)) {
                throw new LedpException(LedpCode.LEDP_16006, new String[] { String.valueOf(modelCommand.getPid()),
                        queryMetadataUrl });
            }
            log.info(metadata);
            String hdfsPath = getHdfsPathForMetadataFile(modelCommand, modelCommandParameters);
            HdfsUtils.writeToFile(yarnConfiguration, hdfsPath, metadata);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_16005, e, new String[] { String.valueOf(modelCommand.getPid()),
                    queryMetadataUrl });
        }
    }

    String getHdfsPathForMetadataFile(ModelCommand modelCommand, ModelCommandParameters modelCommandParameters) {
        String customer = modelCommand.getDeploymentExternalId();
        return customerBaseDir + customer + "/data/" + modelCommandParameters.getEventTable() + "/"
                + modelCommandParameters.getMetadataTable() + "/metadata.avsc";
    }

}
