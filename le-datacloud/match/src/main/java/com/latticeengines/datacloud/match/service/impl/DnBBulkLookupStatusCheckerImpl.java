package com.latticeengines.datacloud.match.service.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;

import com.latticeengines.datacloud.core.service.RateLimitingService;
import com.latticeengines.datacloud.match.service.DnBAuthenticationService;
import com.latticeengines.datacloud.match.service.DnBBulkLookupStatusChecker;
import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBAPIType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.manage.DateTimeUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("dnbBulkLookupStatusChecker")
public class DnBBulkLookupStatusCheckerImpl extends BaseDnBLookupServiceImpl<Map<String, DnBBatchMatchContext>>
        implements DnBBulkLookupStatusChecker {

    private static final Log log = LogFactory.getLog(DnBBulkLookupStatusCheckerImpl.class);

    @Autowired
    private DnBAuthenticationService dnBAuthenticationService;

    @Value("${datacloud.dnb.bulk.getstatus.url.format}")
    private String urlFormat;

    @Value("${datacloud.dnb.retry.maxattempts}")
    private int retries;

    @Value("${datacloud.dnb.application.id}")
    private String applicationId;

    @Value("${datacloud.dnb.authorization.header}")
    private String authorizationHeader;

    @Value("${datacloud.dnb.application.id.header}")
    private String applicationIdHeader;

    @Value("${datacloud.dnb.bulk.getstatus.batchsize}")
    private int checkStatusBatchSize;

    @Value("${datacloud.dnb.bulk.getstatus.transactioncode.xpath}")
    private String transactionCodeXPath;

    @Value("${datacloud.dnb.bulk.getstatus.servicebatchid.xpath}")
    private String serviceBatchIdXpath;

    @Value("${datacloud.dnb.bulk.getstatus.status.xpath}")
    private String statusXpath;

    @Autowired
    private RateLimitingService rateLimitingService;

    @Override
    public List<DnBBatchMatchContext> checkStatus(List<DnBBatchMatchContext> batchContexts) {
        int count = 0;
        StringBuilder sb = new StringBuilder();
        while (count < batchContexts.size()) {
            Map<String, DnBBatchMatchContext> batches = new HashMap<>();
            for (int i = count; i < Math.min(batchContexts.size(), count + checkStatusBatchSize); i++) {
                batches.put(batchContexts.get(i).getServiceBatchId(), batchContexts.get(i));
                count++;
            }
            for (int i = 0; i < retries; i++) {
                RateLimitedAcquisition rlAcq = rateLimitingService.acquireDnBBulkStatus();
                if (!rlAcq.isAllowed()) {
                    logRateLimitingRejection(rlAcq, DnBAPIType.BATCH_STATUS);
                    break;
                }
                executeLookup(batches, DnBKeyType.BATCH, DnBAPIType.BATCH_STATUS);
                sb.delete(0, sb.length());
                for (Map.Entry<String, DnBBatchMatchContext> entry : batches.entrySet()) {
                    sb.append(entry.getValue().getServiceBatchId() + ":" + entry.getValue().getDnbCode().getMessage()
                            + " ");
                }
                if (batches.entrySet().iterator().next().getValue()
                        .getDnbCode() != DnBReturnCode.EXPIRED_TOKEN) {
                    log.info("Checked status for batch requests: " + sb.toString());
                    break;
                }
                dnBAuthenticationService.refreshToken(DnBKeyType.BATCH);
            }
        }
        return batchContexts;
    }

    @Override
    protected String constructUrl(Map<String, DnBBatchMatchContext> batch, DnBAPIType apiType) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, DnBBatchMatchContext> entry : batch.entrySet()) {
            sb.append(entry.getValue().getServiceBatchId() + ".");
        }
        return String.format(urlFormat, sb.substring(0, sb.length() - 1), DateTimeUtils.formatTZ(new Date()));
    }

    @Override
    protected HttpEntity<String> constructEntity(Map<String, DnBBatchMatchContext> batch, String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.add(authorizationHeader, token);
        headers.add(applicationIdHeader, applicationId);
        return new HttpEntity<>("", headers);
    }

    @Override
    protected void parseResponse(String response, Map<String, DnBBatchMatchContext> batches, DnBAPIType apiType) {
        StringBuilder sb = new StringBuilder();
        for (String serviceBatchId : batches.keySet()) {
            sb.append(serviceBatchId + " ");
        }
        DnBReturnCode transactionReturnCode = parseTransactionStatus(response);
        if (transactionReturnCode != DnBReturnCode.OK) {
            updateReturnCodes(batches, transactionReturnCode);
            log.error(
                    String.format("Fail to check status for DnB bulk match requests %s: %s", sb.toString(), response));
            return;
        }
        for (int i = 1; i <= batches.size(); i++) {
            String serviceBatchId = (String) retrieveXmlValueFromResponse(
                    String.format(serviceBatchIdXpath, String.valueOf(i)), response);
            String status = (String) retrieveXmlValueFromResponse(String.format(statusXpath, String.valueOf(i)),
                    response);
            if (StringUtils.isNotEmpty(serviceBatchId) && batches.containsKey(serviceBatchId)) {
                batches.get(serviceBatchId).setDnbCode(parseBatchStatus(status));
            }
        }

    }

    private DnBReturnCode parseTransactionStatus(String body) {
        String code = (String) retrieveXmlValueFromResponse(transactionCodeXPath, body);
        switch (code) {
        case "CM000":
            return DnBReturnCode.OK;
        default:
            return DnBReturnCode.BAD_STATUS;
        }
    }

    private DnBReturnCode parseBatchStatus(String status) {
        switch (status) {
        case "BC005":
        case "BC007":
            return DnBReturnCode.IN_PROGRESS;
        case "BC001":
        case "CM000":
            return DnBReturnCode.OK;
        default:
            return DnBReturnCode.UNKNOWN;
        }
    }

    @Override
    protected void parseError(Exception ex, Map<String, DnBBatchMatchContext> batch) {
        if (ex instanceof HttpClientErrorException) {
            HttpClientErrorException httpEx = (HttpClientErrorException) ex;
            log.error("HttpClientErrorException in DnB batch status checking: " + httpEx.getStatusText());
            for (Map.Entry<String, DnBBatchMatchContext> entry : batch.entrySet()) {
                entry.getValue().setDnbCode(parseDnBHttpError(httpEx));
            }
        } else if (ex instanceof LedpException) {
            LedpException ledpEx = (LedpException) ex;
            log.error("LedpException in DnB batch status checking: " + ledpEx.getCode().getMessage());
            if (ledpEx.getCode() == LedpCode.LEDP_25027) {
                for (Map.Entry<String, DnBBatchMatchContext> entry : batch.entrySet()) {
                    entry.getValue().setDnbCode(DnBReturnCode.EXPIRED_TOKEN);
                }
            } else {
                for (Map.Entry<String, DnBBatchMatchContext> entry : batch.entrySet()) {
                    entry.getValue().setDnbCode(DnBReturnCode.BAD_REQUEST);
                }
            }
        } else {
            log.error("Unhandled exception in DnB batch status checking: " + ex.getMessage());
            ex.printStackTrace();
            for (Map.Entry<String, DnBBatchMatchContext> entry : batch.entrySet()) {
                entry.getValue().setDnbCode(DnBReturnCode.BAD_STATUS);
            }
        }
    }

    private void updateReturnCodes(Map<String, DnBBatchMatchContext> batches, DnBReturnCode code) {
        for (Map.Entry<String, DnBBatchMatchContext> entry : batches.entrySet()) {
            entry.getValue().setDnbCode(code);
        }
    }


}
