package com.latticeengines.datacloud.match.service.impl;

import static org.springframework.http.HttpStatus.OK;

import java.util.ArrayList;
import java.util.Arrays;

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
import com.latticeengines.datacloud.match.exposed.service.DnBRealTimeLookupService;
import com.latticeengines.domain.exposed.datacloud.match.DnBKeyType;
import com.latticeengines.domain.exposed.datacloud.match.DnBMatchEntry;
import com.latticeengines.domain.exposed.datacloud.match.DnBReturnCode;

@Component
public class DnBRealTimeLookupServiceImpl implements DnBRealTimeLookupService {
    private static final Log log = LogFactory.getLog(DnBRealTimeLookupServiceImpl.class);

    @Autowired
    private DnBAuthenticationServiceImpl dnBAuthenticationService;

    @Value("${datacloud.dnb.realtime.url.prefix}")
    private String realTimeUrlPrefix;

    @Value("${datacloud.dnb.authorization.header}")
    private String authorizationHeader;

    @Value("${datacloud.dnb.realtime.duns.jsonpath}")
    private String dunsJsonPath;

    @Value("${datacloud.dnb.realtime.resultid.jsonpath}")
    private String resultIdJsonPath;

    @Value("${datacloud.dnb.realtime.transactionResult.jsonpath}")
    private String transactionResultJsonPath;

    @Value("${datacloud.dnb.realtime.retry.maxattempts}")
    private int retries;

    private RestTemplate restTemplate = new RestTemplate();

    @Override
    public DnBMatchEntry realtimeEntityLookup(DnBMatchEntry input) {
        for (int i = 0; i < retries; i++) {
            DnBReturnCode rc = tryRealtimeEntityLookup(input);
            if (rc != DnBReturnCode.Expired) {
                break;
            }
            dnBAuthenticationService.refreshAndGetToken(DnBKeyType.realtime);
        }

        return input;
    }

    private DnBReturnCode tryRealtimeEntityLookup(DnBMatchEntry input) {
        DnBReturnCode rcCode;
        String token = dnBAuthenticationService.requestToken(DnBKeyType.realtime);
        String url = constructUrl(input);
        try {
            ResponseEntity<String> response = obtainRequestFromDnB(url, token);
            rcCode = parseSucceededResponse(input, response);
        } catch (HttpClientErrorException ex) {
            rcCode = parseDnBHttpError(ex);
            input.setMessages(new ArrayList<>(Arrays.asList(new String[] { rcCode.getMessage() })));
        }

        return rcCode;
    }

    private DnBReturnCode parseDnBHttpError(HttpClientErrorException ex) {
        DnBReturnCode errCode = null;
        Boolean isNeedParseBody = false;
        switch (ex.getStatusCode()) {
        case REQUEST_TIMEOUT:
            errCode = DnBReturnCode.TimeOut;
            break;
        case NOT_FOUND:
            errCode = DnBReturnCode.NoResult;
            break;
        default:
            isNeedParseBody = true;
        }
        if (isNeedParseBody) {
            errCode = parseErrorBody(ex.getResponseBodyAsString());
        }

        return errCode;
    }

    private DnBReturnCode parseErrorBody(String body) {
        DnBReturnCode errCode;
        String dnBErrorCode = retrieveValueFromResponse(resultIdJsonPath, body);
        log.info(body);
        log.info(dnBErrorCode);
        switch (dnBErrorCode) {
        case "SC001":
        case "SC003":
            errCode = DnBReturnCode.Expired;
            break;
        case "SC005":
            errCode = DnBReturnCode.ExceedRequestNum;
            break;
        case "SC006":
            errCode = DnBReturnCode.ExceedConcurrentNum;
            break;
        default:
            errCode = DnBReturnCode.Unknown;
        }

        return errCode;
    }

    @Override
    public DnBMatchEntry realtimeEmailLookup(DnBMatchEntry input) {
        return null;
    }

    private DnBReturnCode parseSucceededResponse(DnBMatchEntry input, ResponseEntity<String> response) {
        if (response.getStatusCode() != OK) {
            return DnBReturnCode.Unknown;
        }

        input.setMessages(new ArrayList<>(Arrays.asList(new String[] { DnBReturnCode.Ok.getMessage() })));
        input.setDuns(retrieveValueFromResponse(dunsJsonPath, response.getBody()));
        return DnBReturnCode.Ok;
    }

    private String constructUrl(DnBMatchEntry input) {
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

    private String retrieveValueFromResponse(String jsonPath, String body) {
        return JsonPath.parse(body).read(jsonPath);
    }
}
