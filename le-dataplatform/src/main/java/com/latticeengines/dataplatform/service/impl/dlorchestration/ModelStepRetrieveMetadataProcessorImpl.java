package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HttpWithRetryUtils;
import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepProcessor;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.visidb.GetQueryMetaDataColumnsRequest;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.remote.exposed.exception.MetadataValidationException;
import com.latticeengines.remote.exposed.service.MetadataValidationService;

@Component("modelStepRetrieveMetadataProcessor")
public class ModelStepRetrieveMetadataProcessorImpl implements ModelStepProcessor {
    public static final String ERROR_MESSAGE_NULL = "\"ErrorMessage\":null";

    private static final Log log = LogFactory.getLog(ModelStepRetrieveMetadataProcessorImpl.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ModelCommandLogService modelCommandLogService;

    @Autowired
    private MetadataValidationService metadataValidationService;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Value("${dataplatform.dlorchestration.metadataerror.fail}")
    private boolean failOnMetadataError;

    private static final String DL_CONFIG_SERVICE_GET_QUERY_META_DATA_COLUMNS = "/GetQueryMetaDataColumns";

    // Make this settable for easier testing
    private String queryMetadataUrlSuffix = DL_CONFIG_SERVICE_GET_QUERY_META_DATA_COLUMNS;

    public void setQueryMetadataUrlSuffix(String queryMetadataUrlSuffix) {
        this.queryMetadataUrlSuffix = queryMetadataUrlSuffix;
    }

    @Override
    public void executeStep(ModelCommand modelCommand, ModelCommandParameters modelCommandParameters) {
        String metadata = executeStepWithResult(modelCommand, modelCommandParameters);
        validateMetadata(metadata, modelCommand, modelCommandParameters);
    }

    @VisibleForTesting
    String executeStepWithResult(ModelCommand modelCommand, ModelCommandParameters modelCommandParameters) {
        String customer = CustomerSpace.parse(modelCommand.getDeploymentExternalId()).toString();
        String deletePath = customerBaseDir + "/" + customer + "/data";
        try (FileSystem fs = FileSystem.get(yarnConfiguration)) {
            if (fs.exists(new Path(deletePath))) {
                boolean result = fs.delete(new Path(deletePath), true);
                if (!result) {
                    throw new LedpException(LedpCode.LEDP_16001, new String[] { deletePath });
                }
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_16001, e, new String[] { deletePath });
        }

        String queryMetadataUrl = modelCommandParameters.getDlUrl() + queryMetadataUrlSuffix;
        String metadata = null;

        GetQueryMetaDataColumnsRequest request = new GetQueryMetaDataColumnsRequest(
                modelCommandParameters.getDlTenant(), modelCommandParameters.getDlQuery());
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("MagicAuthentication", "Security through obscurity!");

        try {
            metadata = HttpWithRetryUtils.executePostRequest(queryMetadataUrl, request, headers);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_16005, e, new String[] { String.valueOf(modelCommand.getPid()),
                    queryMetadataUrl });
        }
        if (Strings.isNullOrEmpty(metadata)) {
            throw new LedpException(LedpCode.LEDP_16006, new String[] { String.valueOf(modelCommand.getPid()),
                    queryMetadataUrl });
        } else if (!metadata.contains(ERROR_MESSAGE_NULL)) {
            modelCommandLogService.log(modelCommand, "Problem with metadata:" + metadata);
            // if system property enabled
            if (failOnMetadataError) {
                throw new LedpException(LedpCode.LEDP_16008, new String[] { metadata });
            }
        }
        log.info(metadata);

        String metadataHdfsPath = getHdfsPathForMetadataFile(modelCommand, modelCommandParameters);
        writeStringToHdfs(metadata, metadataHdfsPath);

        return metadata;
    }

    @VisibleForTesting
    void validateMetadata(String metadata, ModelCommand modelCommand, ModelCommandParameters modelCommandParameters) {
        try {
            metadataValidationService.validate(metadata);
        } catch (MetadataValidationException e) {
            String metadataValidationString = e.getMessage();
            String metadataDiagnosticsHdfsPath = getHdfsPathForMetadataDiagnosticsFile(modelCommand,
                    modelCommandParameters);
            writeStringToHdfs(metadataValidationString, metadataDiagnosticsHdfsPath);
        }
        return;
    }

    @VisibleForTesting
    void writeStringToHdfs(String contents, String hdfsPath) {
        try {
            HdfsUtils.writeToFile(yarnConfiguration, hdfsPath, contents);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_16009, e, new String[] { hdfsPath, contents });
        }
    }

    String getHdfsPathForMetadataFile(ModelCommand modelCommand, ModelCommandParameters modelCommandParameters) {
        String customer = CustomerSpace.parse(modelCommand.getDeploymentExternalId()).toString();
        return String.format("%s/%s/data/%s/metadata.avsc", customerBaseDir, customer,
                modelCommandParameters.getMetadataTable());
    }

    String getHdfsPathForMetadataDiagnosticsFile(ModelCommand modelCommand,
            ModelCommandParameters modelCommandParameters) {
        String customer = CustomerSpace.parse(modelCommand.getDeploymentExternalId()).toString();
        return String.format("%s/%s/data/%s/metadata-diagnostics.json", customerBaseDir, customer,
                modelCommandParameters.getMetadataTable());
    }
}
