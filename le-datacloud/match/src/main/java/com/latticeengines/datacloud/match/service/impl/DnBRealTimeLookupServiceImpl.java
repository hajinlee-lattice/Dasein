package com.latticeengines.datacloud.match.service.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;

import com.latticeengines.datacloud.match.dnb.DnBAPIType;
import com.latticeengines.datacloud.match.dnb.DnBKeyType;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;
import com.latticeengines.datacloud.match.service.DnBMatchResultValidator;
import com.latticeengines.datacloud.match.service.DnBRealTimeLookupService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component
public class DnBRealTimeLookupServiceImpl extends BaseDnBLookupServiceImpl<DnBMatchContext>
        implements DnBRealTimeLookupService {
    private static final Log log = LogFactory.getLog(DnBRealTimeLookupServiceImpl.class);

    @Autowired
    private DnBAuthenticationServiceImpl dnBAuthenticationService;

    @Autowired
    private DnBMatchResultValidator dnbMatchResultValidator;

    @Value("${datacloud.dnb.realtime.url.prefix}")
    private String realTimeUrlPrefix;

    @Value("${datacloud.dnb.realtime.email.lookup.url.format}")
    private String emailLookupUrlFormat;

    @Value("${datacloud.dnb.authorization.header}")
    private String authorizationHeader;

    @Value("${datacloud.dnb.realtime.duns.jsonpath}")
    private String entityDunsJsonPath;

    @Value("${datacloud.dnb.realtime.confidencecode.jsonpath}")
    private String entityConfidenceCodeJsonPath;

    @Value("${datacloud.dnb.realtime.matchgrade.jsonpath}")
    private String entityMatchGradeJsonPath;

    @Value("${datacloud.dnb.realtime.email.duns.jsonpath}")
    private String emailDunsJsonPath;

    @Value("${datacloud.dnb.realtime.retry.maxattempts}")
    private int retries;

    @Override
    public DnBMatchContext realtimeEntityLookup(DnBMatchContext context) {
        for (int i = 0; i < retries; i++) {
            Long startTime = System.currentTimeMillis();
            executeLookup(context, DnBKeyType.REALTIME, DnBAPIType.REALTIME_ENTITY);
            if (context.getDnbCode() != DnBReturnCode.EXPIRED || i == retries - 1) {
                log.info(String.format("DnB realtime entity matching request %s: Status = %s, Duration = %d",
                        context.getLookupRequestId(), context.getDnbCode().getMessage(),
                        System.currentTimeMillis() - startTime));
                break;
            }
            dnBAuthenticationService.refreshAndGetToken(DnBKeyType.REALTIME);
        }
        return context;
    }

    @Override
    public DnBMatchContext realtimeEmailLookup(DnBMatchContext context) {
        for (int i = 0; i < retries; i++) {
            Long startTime = System.currentTimeMillis();
            executeLookup(context, DnBKeyType.REALTIME, DnBAPIType.REALTIME_EMAIL);
            if (context.getDnbCode() != DnBReturnCode.EXPIRED || i == retries - 1) {
                log.info(String.format("DnB realtime email matching request %s: Status = %s, Duration = %d",
                        context.getLookupRequestId(), context.getDnbCode().getMessage(),
                        System.currentTimeMillis() - startTime));
                break;
            }
            dnBAuthenticationService.refreshAndGetToken(DnBKeyType.REALTIME);
        }
        return context;
    }

    @Override
    protected void parseResponse(String response, DnBMatchContext context, DnBAPIType apiType) {
        switch (apiType) {
        case REALTIME_ENTITY:
            context.setDuns((String) retrieveJsonValueFromResponse(entityDunsJsonPath, response));
            context.setConfidenceCode((Integer) retrieveJsonValueFromResponse(entityConfidenceCodeJsonPath, response));
            context.setMatchGrade((String) retrieveJsonValueFromResponse(entityMatchGradeJsonPath, response));
            break;
        case REALTIME_EMAIL:
            context.setDuns((String) retrieveJsonValueFromResponse(emailDunsJsonPath, response));
            break;
        default:
            throw new LedpException(LedpCode.LEDP_25025, new String[] { apiType.name() });
        }
        context.setDnbCode(DnBReturnCode.OK);
        dnbMatchResultValidator.validate(context);
    }
    
    @Override
    protected void parseError(Exception ex, DnBMatchContext context) {
        if (ex instanceof HttpClientErrorException) {
            HttpClientErrorException httpEx = (HttpClientErrorException) ex;
            if (log.isDebugEnabled()) {
                log.debug("HttpClientErrorException in DnB realtime request " + context.getLookupRequestId() + ": "
                        + httpEx.getStatusText());
            }
            context.setDnbCode(parseDnBHttpError(httpEx));
        } else if (ex instanceof LedpException) {
            LedpException ledpEx = (LedpException) ex;
            if (log.isDebugEnabled()) {
                log.debug("LedpException in DnB realtime request " + context.getLookupRequestId() + ": "
                        + ledpEx.getCode().getMessage());
            }
            context.setDnbCode(DnBReturnCode.BAD_REQUEST);
        } else {
            log.error("Unhandled exception in DnB realtime request " + context.getLookupRequestId() + ": "
                    + ex.getMessage());
            ex.printStackTrace();
            context.setDnbCode(DnBReturnCode.UNKNOWN);
        }

    }

    @Override
    protected HttpEntity<String> constructEntity(DnBMatchContext context, String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.add(authorizationHeader, token);
        return new HttpEntity<String>("", headers);
    }

    @Override
    protected String constructUrl(DnBMatchContext context, DnBAPIType apiType) {
        switch (apiType) {
        case REALTIME_ENTITY:
            StringBuilder url = new StringBuilder();
            url.append(realTimeUrlPrefix);
            if (!StringUtils.isEmpty(context.getInputNameLocation().getName())) {
                url.append("SubjectName=");
                url.append(context.getInputNameLocation().getName());
                url.append("&");
            } else {
                throw new LedpException(LedpCode.LEDP_25023);
            }
            if (!StringUtils.isEmpty(context.getInputNameLocation().getCountryCode())) {
                url.append("CountryISOAlpha2Code=");
                url.append(context.getInputNameLocation().getCountryCode());
                url.append("&");
            } else {
                throw new LedpException(LedpCode.LEDP_25023);
            }
            if (!StringUtils.isEmpty(context.getInputNameLocation().getCity())) {
                url.append("PrimaryTownName=");
                url.append(context.getInputNameLocation().getCity());
                url.append("&");
            }
            if (!StringUtils.isEmpty(context.getInputNameLocation().getState())) {
                url.append("TerritoryName=");
                url.append(context.getInputNameLocation().getState());
                url.append("&");
            }
            url.append("cleansematch=true");
            return url.toString();
        case REALTIME_EMAIL:
            if (!StringUtils.isEmpty(context.getInputEmail())) {
                return String.format(emailLookupUrlFormat, context.getInputEmail());
            } else {
                throw new LedpException(LedpCode.LEDP_25024);
            }
        default:
            throw new LedpException(LedpCode.LEDP_25025, new String[] { apiType.name() });
        }

    }
}
