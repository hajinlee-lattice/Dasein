package com.latticeengines.datacloud.match.service.impl;

import static org.springframework.http.HttpStatus.OK;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.util.Base64Utils;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.dnb.DnBBulkMatchInfo;
import com.latticeengines.datacloud.match.dnb.DnBKeyType;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;
import com.latticeengines.datacloud.match.exposed.service.DnBAuthenticationService;
import com.latticeengines.datacloud.match.exposed.service.DnBBulkLookupDispatcher;
import com.latticeengines.domain.exposed.datacloud.manage.DateTimeUtils;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

@Component
public class DnBBulkLookupDispatcherImpl extends BaseDnBLookupServiceImpl<Map<String, MatchKeyTuple>>
        implements DnBBulkLookupDispatcher {

    private static final Log log = LogFactory.getLog(DnBBulkLookupDispatcherImpl.class);

    private static final String DNB_BULK_MATCH_INFO = "DNB_BULK_MATCH_INFO";
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

    @Value("${datacloud.dnb.bulk.timestamp.xpath}")
    private String timestampXpath;

    @Value("${datacloud.dnb.bulk.input.record.format}")
    private String recordFormat;

    private RestTemplate restTemplate = new RestTemplate();

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
    public DnBBulkMatchInfo sendRequest(Map<String, MatchKeyTuple> input) {
        DataFlowContext context = null;
        DnBBulkMatchInfo info = new DnBBulkMatchInfo();
        for (int i = 0; i < retries; i++) {
            context = executeLookup(input, DnBKeyType.BULKMATCH);
            DnBReturnCode returnCode = context.getProperty(DNB_RETURN_CODE, DnBReturnCode.class);
            if (returnCode != DnBReturnCode.EXPIRED) {
                info = context.getProperty(DNB_BULK_MATCH_INFO, DnBBulkMatchInfo.class);
                log.info("Sent batched request to dnb bulk match api, status=" + returnCode + " size=" + input.size()
                        + " timestamp=" + info.getTimestamp() + " serviceId=" + info.getServiceBatchId());
                break;
            }
            dnBAuthenticationService.refreshAndGetToken(DnBKeyType.BULKMATCH);
        }

        info.setDnbCode(context.getProperty(DNB_RETURN_CODE, DnBReturnCode.class));
        info.setLookupRequestIds(new ArrayList<String>(input.keySet()));

        return info;
    }

    @Override
    protected HttpEntity<String> constructEntity(Map<String, MatchKeyTuple> input, String token) {
        String body = constructBulkRequestBody(input);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_XML);
        headers.add(applicationIdHeader, applicationId);
        headers.add(authorizationHeader, token);

        HttpEntity<String> requestEntity = new HttpEntity<>(body, headers);
        return requestEntity;
    }

    @Override
    protected void parseSucceededResponse(ResponseEntity<String> response, DataFlowContext context) {
        if (response.getStatusCode() != OK) {
            context.setProperty(DNB_RETURN_CODE, DnBReturnCode.UNKNOWN);
            return;
        }
        DnBBulkMatchInfo info = new DnBBulkMatchInfo();
        String body = response.getBody();
        info.setServiceBatchId((String) retrieveXmlValueFromResponse(serviceIdXpath, body));
        info.setTimestamp((String) retrieveXmlValueFromResponse(timestampXpath, body));
        info.setDnbCode(DnBReturnCode.OK);

        context.setProperty(DNB_BULK_MATCH_INFO, info);
        context.setProperty(DNB_RETURN_CODE, info.getDnbCode());
    }

    @Override
    protected ResponseEntity<String> sendRequestToDnB(String url, HttpEntity<String> entity) {
        ResponseEntity<String> res = restTemplate.postForEntity(url, entity, String.class);
        return res;
    }

    @Override
    protected String constructUrl(Map<String, MatchKeyTuple> input) {
        return url;
    }

    private String constructBulkRequestBody(Map<String, MatchKeyTuple> input) {
        String createdDateUTC = DateTimeUtils.formatTZ(new Date());

        String tupleStr = convertTuplesToString(input);

        String inputObjectBase64 = Base64Utils.encodeBase64(tupleStr, false, Integer.MAX_VALUE);

        return String.format(dnBBulkApiBody, createdDateUTC.toString(), inputObjectBase64,
                String.valueOf(input.size()));
    }

    private String convertTuplesToString(Map<String, MatchKeyTuple> tuples) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, MatchKeyTuple> entry : tuples.entrySet()) {
            String recordStr = constructOneRecord(entry.getKey(), entry.getValue());
            sb.append(recordStr);
            sb.append("\n");
        }
        return sb.toString();
    }

    private String constructOneRecord(String transactionId, MatchKeyTuple tuple) {
        return String.format(recordFormat, transactionId, normalizeString(tuple.getName()),
                normalizeString(tuple.getCity()), normalizeString(tuple.getState()),
                normalizeString(tuple.getCountry()));
    }
}