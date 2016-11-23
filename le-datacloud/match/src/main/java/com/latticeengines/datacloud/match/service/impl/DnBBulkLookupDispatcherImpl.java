package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;

import com.latticeengines.common.exposed.util.Base64Utils;
import com.latticeengines.datacloud.match.dnb.DnBAPIType;
import com.latticeengines.datacloud.match.dnb.DnBBatchMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBKeyType;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;
import com.latticeengines.datacloud.match.service.DnBAuthenticationService;
import com.latticeengines.datacloud.match.service.DnBBulkLookupDispatcher;
import com.latticeengines.domain.exposed.datacloud.manage.DateTimeUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component
public class DnBBulkLookupDispatcherImpl extends BaseDnBLookupServiceImpl<DnBBatchMatchContext>
        implements DnBBulkLookupDispatcher {

    private static final Log log = LogFactory.getLog(DnBBulkLookupDispatcherImpl.class);

    private static final String DNB_BULK_BODY_FILE_NAME = "com/latticeengines/datacloud/match/BulkApiBodyTemplate.xml";

    @Autowired
    private DnBAuthenticationService dnBAuthenticationService;

    @Value("${datacloud.dnb.bulk.url}")
    private String url;

    @Value("${datacloud.dnb.authorization.header}")
    private String authorizationHeader;

    @Value("${datacloud.dnb.application.id.header}")
    private String applicationIdHeader;

    @Value("${datacloud.dnb.application.id}")
    private String applicationId;

    @Value("${datacloud.dnb.realtime.retry.maxattempts}")
    private int retries;

    @Value("${datacloud.dnb.bulk.servicebatchid.xpath}")
    private String serviceIdXpath;

    @Value("${datacloud.dnb.bulk.input.record.format}")
    private String recordFormat;

    private String dnBBulkApiBody;

    @PostConstruct
    public void initialize() throws IOException {
        InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(DNB_BULK_BODY_FILE_NAME);
        if (is == null) {
            throw new RuntimeException("Cannot find resource " + DNB_BULK_BODY_FILE_NAME);
        }
        dnBBulkApiBody = IOUtils.toString(is, "UTF-8");
    }

    @Override
    public DnBBatchMatchContext sendRequest(DnBBatchMatchContext batchContext) {
        for (int i = 0; i < retries; i++) {
            executeLookup(batchContext, DnBKeyType.BATCH, DnBAPIType.BATCH_DISPATCH);
            if (batchContext.getDnbCode() != DnBReturnCode.EXPIRED_TOKEN || i == retries - 1) {
                log.info("Sent batched request to dnb bulk match api, status=" + batchContext.getDnbCode() + " size="
                        + batchContext.getContexts().size() + " timestamp=" + batchContext.getTimestamp()
                        + " serviceId=" + batchContext.getServiceBatchId());
                break;
            }
            dnBAuthenticationService.refreshToken(DnBKeyType.BATCH);
        }
        return batchContext;
    }

    @Override
    protected void parseError(Exception ex, DnBBatchMatchContext batchContext) {
        if (ex instanceof HttpClientErrorException) {
            HttpClientErrorException httpEx = (HttpClientErrorException) ex;
            if (log.isDebugEnabled()) {
                log.debug("HttpClientErrorException in DnB batch match dispatching request: " + httpEx.getStatusText());
            }
            batchContext.setDnbCode(parseDnBHttpError(httpEx));
        } else if (ex instanceof LedpException) {
            LedpException ledpEx = (LedpException) ex;
            if (log.isDebugEnabled()) {
                log.debug("LedpException in DnB batch match dispatching request: " + ledpEx.getCode().getMessage());
            }
            if (ledpEx.getCode() == LedpCode.LEDP_25027) {
                batchContext.setDnbCode(DnBReturnCode.EXPIRED_TOKEN);
            } else {
                batchContext.setDnbCode(DnBReturnCode.BAD_REQUEST);
            }
        } else {
            log.warn("Unhandled exception in DnB batch match dispatching request: " + ex.getMessage());
            ex.printStackTrace();
            batchContext.setDnbCode(DnBReturnCode.UNKNOWN);
        }

    }


    @Override
    protected void parseResponse(String response, DnBBatchMatchContext batchContext, DnBAPIType apiType) {
        String serviceBatchId = (String) retrieveXmlValueFromResponse(serviceIdXpath, response);
        if (!StringUtils.isEmpty(serviceBatchId)) {
            batchContext.setServiceBatchId(serviceBatchId);
            batchContext.setDnbCode(DnBReturnCode.OK);
        } else {
            log.warn(String.format("Fail to extract serviceBatchId from response of DnB bulk match request: %s",
                    response));
            batchContext.setDnbCode(DnBReturnCode.BAD_RESPONSE);
        }
    }

    @Override
    protected HttpEntity<String> constructEntity(DnBBatchMatchContext batchContext, String token) {
        String body = constructBulkRequestBody(batchContext);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_XML);
        headers.add(applicationIdHeader, applicationId);
        headers.add(authorizationHeader, token);

        HttpEntity<String> requestEntity = new HttpEntity<>(body, headers);
        return requestEntity;
    }

    @Override
    protected String constructUrl(DnBBatchMatchContext batchContext, DnBAPIType apiType) {
        return url;
    }

    private String constructBulkRequestBody(DnBBatchMatchContext batchContext) {
        Date now = new Date();
        batchContext.setTimestamp(now);
        String createdDateUTC = DateTimeUtils.formatTZ(now);
        String tupleStr = convertTuplesToString(batchContext);
        String inputObjectBase64 = Base64Utils.encodeBase64(tupleStr, false, Integer.MAX_VALUE);
        log.info(String.format("Submitted encoded match input: %s", inputObjectBase64));
        return String.format(dnBBulkApiBody, createdDateUTC.toString(), inputObjectBase64,
                String.valueOf(batchContext.getContexts().size()));
    }

    private String convertTuplesToString(DnBBatchMatchContext batchContext) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, DnBMatchContext> entry : batchContext.getContexts().entrySet()) {
            String recordStr = constructOneRecord(entry.getKey(), entry.getValue());
            sb.append(recordStr);
            sb.append("\n");
        }
        return sb.toString();
    }

    private String constructOneRecord(String transactionId, DnBMatchContext matchContext) {
        return String.format(recordFormat, transactionId,
                StringUtils.defaultIfEmpty(matchContext.getInputNameLocation().getName(), ""),
                StringUtils.defaultIfEmpty(matchContext.getInputNameLocation().getCity(), ""),
                StringUtils.defaultIfEmpty(matchContext.getInputNameLocation().getState(), ""),
                StringUtils.defaultIfEmpty(matchContext.getInputNameLocation().getCountryCode(), ""));
    }
}