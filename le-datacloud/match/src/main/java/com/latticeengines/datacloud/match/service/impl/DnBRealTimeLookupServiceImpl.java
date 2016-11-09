package com.latticeengines.datacloud.match.service.impl;

import static org.springframework.http.HttpStatus.OK;

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

import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.dnb.DnBKeyType;
import com.latticeengines.datacloud.match.dnb.DnBMatchOutput;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;
import com.latticeengines.datacloud.match.exposed.service.DnBRealTimeLookupService;
import com.latticeengines.datacloud.match.service.DnBMatchResultValidator;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

@Component
public class DnBRealTimeLookupServiceImpl extends BaseDnBLookupServiceImpl<MatchKeyTuple>
        implements DnBRealTimeLookupService {
    private static final Log log = LogFactory.getLog(DnBRealTimeLookupServiceImpl.class);

    private static final String DNB_MATCH_OUTPUT = "DNB_MATCH_OUTPUT";

    @Autowired
    private DnBAuthenticationServiceImpl dnBAuthenticationService;

    @Autowired
    private DnBMatchResultValidator dnbMatchResultValidator;

    @Value("${datacloud.dnb.realtime.url.prefix}")
    private String realTimeUrlPrefix;

    @Value("${datacloud.dnb.authorization.header}")
    private String authorizationHeader;

    @Value("${datacloud.dnb.realtime.duns.jsonpath}")
    private String dunsJsonPath;

    @Value("${datacloud.dnb.realtime.confidencecode.jsonpath}")
    private String confidenceCodeJsonPath;

    @Value("${datacloud.dnb.realtime.matchgrade.jsonpath}")
    private String matchGradeJsonPath;

    @Value("${datacloud.dnb.realtime.resultid.jsonpath}")
    private String resultIdJsonPath;

    @Value("${datacloud.dnb.realtime.transactionResult.jsonpath}")
    private String transactionResultJsonPath;

    @Value("${datacloud.dnb.realtime.retry.maxattempts}")
    private int retries;

    private RestTemplate restTemplate = new RestTemplate();

    @Override
    public DnBMatchOutput realtimeEntityLookup(MatchKeyTuple input) {
        DataFlowContext context = null;
        DnBMatchOutput output = new DnBMatchOutput();
        for (int i = 0; i < retries; i++) {
            context = executeLookup(input, DnBKeyType.REALTIME);
            DnBReturnCode returnCode = context.getProperty(DNB_RETURN_CODE, DnBReturnCode.class);
            if (returnCode != DnBReturnCode.EXPIRED) {
                output = context.getProperty(DNB_MATCH_OUTPUT, DnBMatchOutput.class);
                log.debug("Finished dnb realtime lookup request status= " + returnCode);
                break;
            }
            dnBAuthenticationService.refreshAndGetToken(DnBKeyType.REALTIME);
        }

        output.setDnbCode(context.getProperty(DNB_RETURN_CODE, DnBReturnCode.class));

        return output;
    }

    @Override
    public DnBMatchOutput realtimeEmailLookup(MatchKeyTuple input) {
        return null;
    }

    @Override
    protected HttpEntity<String> constructEntity(MatchKeyTuple input, String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.add(authorizationHeader, token);
        return new HttpEntity<>("", headers);
    }

    @Override
    protected void parseSucceededResponse(ResponseEntity<String> response, DataFlowContext context) {
        if (response.getStatusCode() != OK) {
            context.setProperty(DNB_RETURN_CODE, DnBReturnCode.UNKNOWN);
            return;
        }
        String body = response.getBody();
        DnBMatchOutput output = new DnBMatchOutput();
        output.setDuns((String) retrieveJsonValueFromResponse(dunsJsonPath, body));
        output.setConfidenceCode((Integer) retrieveJsonValueFromResponse(confidenceCodeJsonPath, body));
        output.setMatchGrade((String) retrieveJsonValueFromResponse(matchGradeJsonPath, body));
        output.setDnbCode(DnBReturnCode.OK);
        dnbMatchResultValidator.validate(output);
        context.setProperty(DNB_MATCH_OUTPUT, output);
        context.setProperty(DNB_RETURN_CODE, output.getDnbCode());
    }

    @Override
    protected ResponseEntity<String> sendRequestToDnB(String url, HttpEntity<String> entity) {
        ResponseEntity<String> res = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);
        return res;
    }

    @Override
    protected String constructUrl(MatchKeyTuple tuple) {
        StringBuilder url = new StringBuilder();
        url.append(realTimeUrlPrefix);
        String normalizedstr = normalizeString(tuple.getName());
        if (normalizedstr.length() != 0) {
            url.append("SubjectName=");
            url.append(normalizedstr);
            url.append("&");
        }
        normalizedstr = normalizeString(tuple.getCountryCode());
        if (normalizedstr.length() != 0) {
            url.append("CountryISOAlpha2Code=");
            url.append(normalizedstr);
            url.append("&");
        }
        normalizedstr = normalizeString(tuple.getCity());
        if (normalizedstr.length() != 0) {
            url.append("PrimaryTownName=");
            url.append(normalizedstr);
            url.append("&");
        }
        normalizedstr = normalizeString(tuple.getState());
        if (normalizedstr.length() != 0) {
            url.append("TerritoryName=");
            url.append(normalizedstr);
            url.append("&");
        }
        url.append("cleansematch=true");
        return url.toString();
    }
}
