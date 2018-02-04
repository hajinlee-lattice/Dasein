package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepProcessor;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.visidb.GetQueryMetaDataColumnsRequest;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.remote.exposed.exception.MetadataValidationException;
import com.latticeengines.remote.exposed.service.MetadataValidationService;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;

@Component("modelStepRetrieveMetadataProcessor")
public class ModelStepRetrieveMetadataProcessorImpl implements ModelStepProcessor {
    public static final String ERROR_MESSAGE_NULL = "\"ErrorMessage\":null";

    private static final Logger log = LoggerFactory.getLogger(ModelStepRetrieveMetadataProcessorImpl.class);

    private static final String METADATA_DIAGNOSTIC_FILE = "metadata-diagnostics.json";
    private static final String METADATA_FILE = "metadata.avsc";

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private ModelCommandLogService modelCommandLogService;

    @Inject
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
        RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
        MagicAuthenticationHeaderHttpRequestInterceptor authHeader = new MagicAuthenticationHeaderHttpRequestInterceptor();
        List<ClientHttpRequestInterceptor> interceptors = new ArrayList<>(restTemplate.getInterceptors());
        interceptors.removeIf(i -> i instanceof MagicAuthenticationHeaderHttpRequestInterceptor);
        interceptors.add(authHeader);
        restTemplate.setInterceptors(interceptors);

        try {
            metadata = restTemplate.postForObject(queryMetadataUrl, request, String.class);
        } catch (Exception e) {
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
        return String.format("%s/%s/data/%s/%s", customerBaseDir, customer, modelCommandParameters.getMetadataTable(),
                METADATA_FILE);
    }

    String getHdfsPathForMetadataDiagnosticsFile(ModelCommand modelCommand,
            ModelCommandParameters modelCommandParameters) {
        String customer = CustomerSpace.parse(modelCommand.getDeploymentExternalId()).toString();
        return String.format("%s/%s/data/%s/%s", customerBaseDir, customer, modelCommandParameters.getMetadataTable(),
                METADATA_DIAGNOSTIC_FILE);
    }
}
