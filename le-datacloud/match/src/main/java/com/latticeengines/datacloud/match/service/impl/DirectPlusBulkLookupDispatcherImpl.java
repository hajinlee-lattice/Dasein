package com.latticeengines.datacloud.match.service.impl;

import java.io.StringWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.datacloud.core.service.RateLimitingService;
import com.latticeengines.datacloud.match.service.DnBBulkLookupDispatcher;
import com.latticeengines.datacloud.match.util.DirectPlusUtils;
import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBAPIType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.proxy.exposed.RestApiClient;

import au.com.bytecode.opencsv.CSVWriter;

@Component("directPlusBulkLookupDispatcher")
public class DirectPlusBulkLookupDispatcherImpl extends BaseDnBBulkLookupDispatcherImpl implements DnBBulkLookupDispatcher {

    private static final Logger log = LoggerFactory.getLogger(DirectPlusBulkLookupDispatcherImpl.class);

    @Inject
    private RateLimitingService rateLimitingService;

    @Inject
    private DirectPlusBulkLookupDispatcherImpl _self;

    @Inject
    private ApplicationContext applicationContext;

    @Value("${datacloud.dnb.direct.plus.submit.batch.url}")
    private String url;

    @Value("${datacloud.dnb.authorization.header}")
    private String authorizationHeader;

    @Value("${datacloud.dnb.direct.plus.errorcode.jsonpath}")
    private String errorCodeXpath;

    @Value("${datacloud.dnb.direct.plus.batch.s3.key}")
    private String s3Key;

    @Value("${datacloud.dnb.direct.plus.batch.s3.key.md5.encrypted}")
    private String s3KeyMd5;

    private RestApiClient uploadClient;

    @Override
    protected DirectPlusBulkLookupDispatcherImpl self() {
        return _self;
    }

    @Override
    protected String url() {
        return url;
    }

    @Override
    protected DnBKeyType keyType() {
        return DnBKeyType.DPLUS;
    }

    @Override
    protected String getErrorCodePath() {
        return errorCodeXpath;
    }

    @Override
    protected ResponseType getResponseType() {
        return ResponseType.JSON;
    }

    @Override
    protected RateLimitedAcquisition acquireQuota(long numRows, boolean attemptOnly) {
        return rateLimitingService.acquireDirectPlusBatchRequest(attemptOnly);
    }

    @Override
    protected void parseResponse(String response, DnBBatchMatchContext batchContext) {
        if (StringUtils.isNotBlank(response)) {
            JsonNode jsonNode = JsonUtils.deserialize(response, JsonNode.class);
            String jobId = JsonUtils.parseStringValueAtPath(jsonNode, "jobID");
            log.info("Get Multi-Process JobID={}", jobId);
            if (StringUtils.isNotBlank(jobId)) {
                String contentUrl = JsonUtils.parseStringValueAtPath(jsonNode, "jobSubmissionDetail", "contentURL");
                try {
                    uploadFile(contentUrl, batchContext);
                    batchContext.setTimestamp(new Date());
                    batchContext.setServiceBatchId(jobId);
                    batchContext.setDnbCode(DnBReturnCode.SUBMITTED);
                } catch (Exception e) {
                    log.error("Failed to upload input file.", e);
                }
                return;
            }
        }
        log.error("Fail to extract serviceBatchId from response of DnB bulk match request: {}", response);
        batchContext.setDnbCode(DnBReturnCode.BAD_RESPONSE);
    }

    @Override
    protected void executeLookup(DnBBatchMatchContext batchContext) {
        executeLookup(batchContext, keyType(), DnBAPIType.BATCH_DISPATCH);
    }

    @Override
    protected HttpEntity<String> constructEntity(DnBBatchMatchContext batchContext, String token) {
        ObjectNode objectNode = JsonUtils.createObjectNode();
        objectNode.put("customerKey", s3Key);
        objectNode.put("processId", "match");
        objectNode.put("processVersion", "v2");
        objectNode.put("inputFileName", NamingUtils.randomSuffix("lattice_", 6));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.add(authorizationHeader, "Bearer " + token);
        return new HttpEntity<>(JsonUtils.serialize(objectNode), headers);
    }

    private void uploadFile(String contentUrl, DnBBatchMatchContext batchContext) {
        Map<String, String> headers = new HashMap<>();
        headers.put("x-amz-server-side-encryption-customer-key", s3Key);
        headers.put("x-amz-server-side-encryption-customer-algorithm", "AES256");
        headers.put("x-amz-server-side-encryption-customer-key-MD5", s3KeyMd5);
        headers.put("Content-Type", "text/csv");
        String content = getUploadContentStr(batchContext);
        getUploadClient().put(contentUrl, content, headers);
    }

    private String getUploadContentStr(DnBBatchMatchContext batchContext) {
        StringWriter writer = new StringWriter();
        CSVWriter csvWriter = new CSVWriter(writer);
        batchContext.getContexts().forEach((id, ctx) -> {
            String params = DirectPlusUtils.constructUrlParams(ctx, DnBAPIType.REALTIME_ENTITY);
            csvWriter.writeNext(new String[] { id, params });
        });
        return writer.toString();
    }

    private synchronized RestApiClient getUploadClient() {
        if (uploadClient ==  null) {
            uploadClient = RestApiClient.newExternalClient(applicationContext);
            uploadClient.setErrorHandler(new GetDnBResponseErrorHandler());
            uploadClient.setUseUri(true);
        }
        return uploadClient;
    }

}
