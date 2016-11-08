package com.latticeengines.datacloud.match.service.impl;

import static org.springframework.http.HttpStatus.OK;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.util.Base64Utils;
import com.latticeengines.datacloud.match.dnb.DnBBulkMatchInfo;
import com.latticeengines.datacloud.match.dnb.DnBKeyType;
import com.latticeengines.datacloud.match.dnb.DnBMatchOutput;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;
import com.latticeengines.datacloud.match.exposed.service.DnBAuthenticationService;
import com.latticeengines.datacloud.match.exposed.service.DnBBulkLookupFetcher;
import com.latticeengines.datacloud.match.service.DnBMatchResultValidator;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

@Component
public class DnBBulkLookupFetcherImpl extends BaseDnBLookupServiceImpl<DnBBulkMatchInfo>
        implements DnBBulkLookupFetcher {

    private static final Log log = LogFactory.getLog(DnBBulkLookupFetcherImpl.class);

    private static final String DNB_MATCH_OUTPUT_LIST = "DNB_MATCH_OUTPUT_LIST";

    private static final String DNB_GET_RESULT_URL = "https://direct.dnb.com:8443/V3.0/Batches/%s?SubmittingOfficeID=%s&ServiceVersionNumber=%s&ApplicationTransactionID=%s&TransactionTimestamp=%s";

    @Autowired
    private DnBAuthenticationService dnBAuthenticationService;

    @Autowired
    private DnBMatchResultValidator dnbMatchResultValidator;

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

    @Value("${datacloud.dnb.bulk.output.content.object.xpath}")
    private String contentObjectXpath;

    @Value("${datacloud.dnb.bulk.result.id.xpath}")
    private String batchResultIdXpath;

    @Value("${datacloud.dnb.bulk.query.interval}")
    private int queryInterval;

    @Value("${datacloud.dnb.bulk.office.id}")
    private int officeID;

    @Value("${datacloud.dnb.bulk.service.number}")
    private int serviceNumber;

    @Value("${datacloud.dnb.realtime.resultid.jsonpath}")
    private String resultIdJsonPath;

    private RestTemplate restTemplate = new RestTemplate();

    private static Date timeAnchor = new Date(1);

    @Override
    public List<DnBMatchOutput> getResult(DnBBulkMatchInfo info) {

        DataFlowContext context = new DataFlowContext();
        if (!preValidation()) {
            context.setProperty(DNB_RETURN_CODE, DnBReturnCode.RATE_LIMITING);
            return null;
        }

        List<DnBMatchOutput> output = null;
        for (int i = 0; i < retries; i++) {
            context = executeLookup(info, DnBKeyType.bulkmatch);
            DnBReturnCode returnCode = context.getProperty(DNB_RETURN_CODE, DnBReturnCode.class);
            if (returnCode != DnBReturnCode.EXPIRED) {
                info.setDnbCode(returnCode);
                List<?> outputUncheck = context.getProperty(DNB_MATCH_OUTPUT_LIST, List.class);
                for(Object obj: outputUncheck) {
                    output.add((DnBMatchOutput) obj);
                }
                break;
            }
            dnBAuthenticationService.refreshAndGetToken(DnBKeyType.bulkmatch);
        }

        return output;
    }

    private boolean preValidation() {
        Date now = new Date();
        if ((now.getTime() - timeAnchor.getTime()) / (1000 * 60) < queryInterval) {
            return false;
        }
        timeAnchor = now;
        return true;
    }

    @Override
    protected HttpEntity<String> constructEntity(DnBBulkMatchInfo input, String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.add(authorizationHeader, token);
        headers.add(applicationIdHeader, applicationId);
        return new HttpEntity<>("", headers);
    }

    @Override
    protected void parseSucceededResponse(ResponseEntity<String> response, DataFlowContext context) {
        if (response.getStatusCode() != OK) {
            context.setProperty(DNB_RETURN_CODE, DnBReturnCode.UNKNOWN);
            return;
        }

        String body = response.getBody();
        DnBReturnCode returnCode = parseBatchProcessStatus(body);

        if (returnCode != DnBReturnCode.OK) {
            context.setProperty(DNB_RETURN_CODE, returnCode);
            return;
        }

        List<DnBMatchOutput> outputList = new ArrayList<>();
        String encodedStr = (String) retrieveXmlValueFromResponse(contentObjectXpath, body);
        byte[] decodeResults = Base64Utils.decodeBase64(encodedStr);
        List<String> resultsList = Arrays.asList((new String(decodeResults)).split("\n"));
        for (String result : resultsList) {
            outputList.add(normalizeOneRecord(result));
        }

        context.setProperty(DNB_MATCH_OUTPUT_LIST, outputList);
    }

    private DnBReturnCode parseBatchProcessStatus(String body) {
        DnBReturnCode returnCode;
        String dnBReturnCode = (String) retrieveXmlValueFromResponse(batchResultIdXpath, body);
        switch (dnBReturnCode) {
        case "BC005":
        case "BC007":
            returnCode = DnBReturnCode.IN_PROGRESS;
            break;
        case "CM000":
            returnCode = DnBReturnCode.OK;
            break;
        default:
            returnCode = DnBReturnCode.UNKNOWN;
        }

        return returnCode;
    }

    private DnBMatchOutput normalizeOneRecord(String record) {
        DnBMatchOutput output = new DnBMatchOutput();
        record = record.substring(1, record.length()-1);
        String[] values = record.split("\",\"");

        String duns = values[25];
        if(!StringUtils.isNumeric(values[48])) {
            output.setDnbCode(DnBReturnCode.DISCARD);
            return output;
        }
        int confidenceCode = Integer.parseInt(values[48]);
        String matchGrade = values[49];

        output.setDuns(duns);
        output.setConfidenceCode(confidenceCode);
        output.setMatchGrade(matchGrade);
        output.setDnbCode(DnBReturnCode.OK);
        dnbMatchResultValidator.validate(output);
        return output;
    }

    @Override
    protected ResponseEntity<String> sendRequestToDnB(String url, HttpEntity<String> entity) {
        ResponseEntity<String> res = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);
        return res;
    }

    @Override
    protected String constructUrl(DnBBulkMatchInfo info) {
        return String.format(DNB_GET_RESULT_URL, info.getServiceBatchId(), officeID, serviceNumber,
                info.getApplicationId(), info.getTimestamp());
    }
}
