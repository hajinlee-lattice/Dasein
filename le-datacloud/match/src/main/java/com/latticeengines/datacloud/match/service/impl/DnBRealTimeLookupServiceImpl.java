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
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import com.jayway.jsonpath.JsonPath;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.dnb.DnBKeyType;
import com.latticeengines.datacloud.match.dnb.DnBMatchOutput;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;
import com.latticeengines.datacloud.match.exposed.service.DnBRealTimeLookupService;
import com.latticeengines.datacloud.match.service.DnBMatchResultValidator;

@Component
public class DnBRealTimeLookupServiceImpl implements DnBRealTimeLookupService {
    private static final Log log = LogFactory.getLog(DnBRealTimeLookupServiceImpl.class);

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
        DnBMatchOutput res = null;
        for (int i = 0; i < retries; i++) {
            res = tryRealtimeEntityLookup(input);
            if (res.getDnbCode() != DnBReturnCode.EXPIRED) {
                break;
            }
            dnBAuthenticationService.refreshAndGetToken(DnBKeyType.realtime);
        }

        return res;
    }

    private DnBMatchOutput tryRealtimeEntityLookup(MatchKeyTuple input) {
        DnBMatchOutput res = new DnBMatchOutput();
        String token = dnBAuthenticationService.requestToken(DnBKeyType.realtime);
        String url = constructUrl(input);
        try {
            ResponseEntity<String> response = obtainRequestFromDnB(url, token);
            parseSucceededResponse(input, response, res);
        } catch (HttpClientErrorException ex) {
            parseDnBHttpError(ex, res);
        }

        return res;
    }

    private void parseDnBHttpError(HttpClientErrorException ex, DnBMatchOutput res) {
        DnBReturnCode errCode = null;
        Boolean isNeedParseBody = false;
        switch (ex.getStatusCode()) {
        case REQUEST_TIMEOUT:
            errCode = DnBReturnCode.TIMEOUT;
            break;
        case NOT_FOUND:
            errCode = DnBReturnCode.NO_RESULT;
            break;
        default:
            isNeedParseBody = true;
        }
        if (isNeedParseBody) {
            errCode = parseErrorBody(ex.getResponseBodyAsString());
        }

        res.setDnbCode(errCode);
    }

    private DnBReturnCode parseErrorBody(String body) {
        DnBReturnCode errCode;
        String dnBErrorCode = (String) retrieveValueFromResponse(resultIdJsonPath, body);
        // log.info(body);
        // log.info(dnBErrorCode);
        switch (dnBErrorCode) {
        case "SC001":
        case "SC003":
            errCode = DnBReturnCode.EXPIRED;
            break;
        case "SC005":
            errCode = DnBReturnCode.EXCEED_REQUEST_NUM;
            break;
        case "SC006":
            errCode = DnBReturnCode.EXCEED_CONCURRENT_NUM;
            break;
        default:
            errCode = DnBReturnCode.UNKNOWN;
        }

        return errCode;
    }

    @Override
    public DnBMatchOutput realtimeEmailLookup(MatchKeyTuple input) {
        return null;
    }

    private void parseSucceededResponse(MatchKeyTuple input, ResponseEntity<String> response,
            DnBMatchOutput res) {
        if (response.getStatusCode() != OK) {
            res.setDnbCode(DnBReturnCode.UNKNOWN);
            return;
        }
        res.setDuns((String) retrieveValueFromResponse(dunsJsonPath, response.getBody()));
        res.setConfidenceCode((Integer) retrieveValueFromResponse(confidenceCodeJsonPath, response.getBody()));
        res.setMatchGrade((String) retrieveValueFromResponse(matchGradeJsonPath, response.getBody()));
        res.setDnbCode(DnBReturnCode.OK);
        dnbMatchResultValidator.validate(res);
    }

    private String constructUrl(MatchKeyTuple input) {
        StringBuilder url = new StringBuilder();
        url.append(realTimeUrlPrefix);
        String normalizedstr = normalizeString(input.getName());
        if (normalizedstr.length() != 0) {
            url.append("SubjectName=");
            url.append(normalizedstr);
            url.append("&");
        }
        normalizedstr = normalizeString(input.getCountryCode());
        if (normalizedstr.length() != 0) {
            url.append("CountryISOAlpha2Code=");
            url.append(normalizedstr);
            url.append("&");
        }
        normalizedstr = normalizeString(input.getCity());
        if (normalizedstr.length() != 0) {
            url.append("PrimaryTownName=");
            url.append(normalizedstr);
            url.append("&");
        }
        normalizedstr = normalizeString(input.getState());
        if (normalizedstr.length() != 0) {
            url.append("TerritoryName=");
            url.append(normalizedstr);
            url.append("&");
        }
        url.append("cleansematch=true");
        return url.toString();
    }

    private String normalizeString(String str) {
        if (str == null) {
            return "";
        }

        return str.trim();
    }

    private HttpEntity<String> realTimeRequestEntity(String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.add(authorizationHeader, token);
        return new HttpEntity<>("", headers);
    }

    private ResponseEntity<String> obtainRequestFromDnB(String url, String token) {
        ResponseEntity<String> res = restTemplate.exchange(url, HttpMethod.GET, realTimeRequestEntity(token), String.class);
        return res;
    }

    private Object retrieveValueFromResponse(String jsonPath, String body) {
        return JsonPath.parse(body).read(jsonPath);
    }
}
