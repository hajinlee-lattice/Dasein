package com.latticeengines.datacloud.match.service.impl;

import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.service.DnBBulkLookupFetcher;
import com.latticeengines.datacloud.match.util.DirectPlusUtils;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBAPIType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;

@Component("directPlusBulkLookupFetcher")
public class DirectPlusBulkLookupFetcherImpl extends BaseDnBBulkLookupFetcherImpl implements DnBBulkLookupFetcher {

    private static final Logger log = LoggerFactory.getLogger(DirectPlusBulkLookupFetcherImpl.class);

    @Inject
    private DirectPlusBulkLookupFetcherImpl _self;

    @Value("${datacloud.dnb.direct.plus.errorcode.jsonpath}")
    private String errorCodeXpath;

    @Value("${datacloud.dnb.realtime.operatingstatus.outofbusiness}")
    private String outOfBusinessValue;

    @Value("${datacloud.dnb.direct.plus.batch.s3.key}")
    private String s3Key;

    @Value("${datacloud.dnb.direct.plus.batch.s3.key.md5.encrypted}")
    private String s3KeyMd5;

    @Override
    protected DirectPlusBulkLookupFetcherImpl self() {
        return _self;
    }

    @Override
    protected void executeLookup(DnBBatchMatchContext batchContext) {
        executeLookup(batchContext, keyType(), DnBAPIType.BATCH_FETCH);
    }

    @Override
    protected DnBKeyType keyType() {
        return DnBKeyType.DPLUS;
    }

    @Override
    protected ResponseType getResponseType() {
        return ResponseType.CSV;
    }

    @Override
    protected String getErrorCodePath() {
        return errorCodeXpath;
    }

    @Override
    protected String constructUrl(DnBBatchMatchContext batchContext) {
        String resultUrl = batchContext.getResultUrl();
        if (StringUtils.isBlank(resultUrl)) {
            throw new IllegalArgumentException("Result URL is not found in batch context.");
        }
        return resultUrl;
    }

    @Override
    protected HttpEntity<String> constructEntity(DnBBatchMatchContext batchContext, String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("x-amz-server-side-encryption-customer-key", s3Key);
        headers.set("x-amz-server-side-encryption-customer-algorithm", "AES256");
        headers.set("x-amz-server-side-encryption-customer-key-MD5", s3KeyMd5);
        return new HttpEntity<>("", headers);
    }

    @Override
    protected void parseResponse(String response, DnBBatchMatchContext batchContext) {
        try {
            batchContext.setDuration(System.currentTimeMillis() - batchContext.getTimestamp().getTime());
            Reader reader = new InputStreamReader(IOUtils.toInputStream(response, Charset.defaultCharset()));
            IOUtils.lineIterator(reader).forEachRemaining(line -> {
                String recordId = line.substring(0, line.indexOf(","));
                line = line.replaceFirst(recordId + ",", "").trim();
                String status = line.substring(0, line.indexOf(","));
                String result = line.replaceFirst(status + ",", "").trim();
                DnBMatchContext output = parseMatchResult(status, result);
                DnBMatchContext context = batchContext.getContexts().get(recordId);
                context.copyMatchResult(output);
            });
            for (String lookupRequestId : batchContext.getContexts().keySet()) {
                DnBMatchContext context = batchContext.getContexts().get(lookupRequestId);
                context.setDuration(batchContext.getDuration());
                context.setServiceBatchId(batchContext.getServiceBatchId());
                if (context.getDnbCode() == null) {
                    context.setDnbCode(DnBReturnCode.UNMATCH);
                }
            }
        } catch (Exception ex) {
            log.error(String.format("Fail to extract match result from response of DnB bulk match request %s: %s",
                    batchContext.getServiceBatchId(), response), ex);
            batchContext.setDnbCode(DnBReturnCode.BAD_RESPONSE);
        }

    }

    private DnBMatchContext parseMatchResult(String status, String resultStr) {
        DnBMatchContext output = new DnBMatchContext();
        output.setDnbCode(DnBReturnCode.UNMATCH);
        if (StringUtils.isNotBlank(resultStr) && "200".equals(status)) {
            try {
                DirectPlusUtils.parseJsonResponse(resultStr, output, DnBAPIType.REALTIME_ENTITY);
                if (CollectionUtils.isNotEmpty(output.getCandidates())) {
                    DnBMatchCandidate topCandidate = output.getCandidates().get(0);
                    output.setDuns(topCandidate.getDuns());
                    output.setOrigDuns(topCandidate.getDuns());
                    output.setConfidenceCode(topCandidate.getMatchInsight().getConfidenceCode());
                    output.setMatchInsight(topCandidate.getMatchInsight());
                    output.setMatchGrade(topCandidate.getMatchInsight().getMatchGrade());
                    output.setMatchedNameLocation(topCandidate.getNameLocation());
                    output.setOutOfBusiness(outOfBusinessValue.equalsIgnoreCase(topCandidate.getOperatingStatus()));
                    output.setDnbCode(DnBReturnCode.OK);
                }
            } catch (Exception e) {
                log.warn("Failed to parse Multi-Process result [{}]", resultStr, e);
            }
        }
        return output;
    }

}
