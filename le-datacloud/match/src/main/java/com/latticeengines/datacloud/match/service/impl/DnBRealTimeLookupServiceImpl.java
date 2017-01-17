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
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
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

    @Value("${datacloud.dnb.realtime.name.jsonpath}")
    private String entityNameJsonPath;

    @Value("${datacloud.dnb.realtime.street.jsonpath}")
    private String entityStreetJsonPath;

    @Value("${datacloud.dnb.realtime.city.jsonpath}")
    private String entityCityJsonPath;

    @Value("${datacloud.dnb.realtime.state.jsonpath}")
    private String entityStateJsonPath;

    @Value("${datacloud.dnb.realtime.countrycode.jsonpath}")
    private String entityCountryCodeJsonPath;

    @Value("${datacloud.dnb.realtime.zipcode.jsonpath}")
    private String entityZipCodeJsonPath;

    @Value("${datacloud.dnb.realtime.phonenumber.jsonpath}")
    private String entityPhoneNumberJsonPath;

    @Value("${datacloud.dnb.realtime.confidencecode.jsonpath}")
    private String entityConfidenceCodeJsonPath;

    @Value("${datacloud.dnb.realtime.matchgrade.jsonpath}")
    private String entityMatchGradeJsonPath;

    @Value("${datacloud.dnb.realtime.email.duns.jsonpath}")
    private String emailDunsJsonPath;

    @Value("${datacloud.dnb.realtime.retry.maxattempts}")
    private int retries;

    @Value("${datacloud.dnb.realtime.reasoncode.de}")
    private String reasonCodeDe;

    @Override
    public DnBMatchContext realtimeEntityLookup(DnBMatchContext context) {
        for (int i = 0; i < retries; i++) {
            Long startTime = System.currentTimeMillis();
            executeLookup(context, DnBKeyType.REALTIME, DnBAPIType.REALTIME_ENTITY);
            context.setDuration(System.currentTimeMillis() - startTime);
            if (context.getDnbCode() != DnBReturnCode.EXPIRED_TOKEN || i == retries - 1) {
                log.info(String.format("DnB realtime entity matching request %s: Status = %s, Duration = %d",
                        context.getLookupRequestId(), context.getDnbCode().getMessage(), context.getDuration()));
                break;
            }
            dnBAuthenticationService.refreshToken(DnBKeyType.REALTIME);
        }
        return context;
    }

    @Override
    public DnBMatchContext realtimeEmailLookup(DnBMatchContext context) {
        for (int i = 0; i < retries; i++) {
            Long startTime = System.currentTimeMillis();
            executeLookup(context, DnBKeyType.REALTIME, DnBAPIType.REALTIME_EMAIL);
            context.setDuration(System.currentTimeMillis() - startTime);
            if (context.getDnbCode() != DnBReturnCode.EXPIRED_TOKEN || i == retries - 1) {
                log.info(String.format("DnB realtime email matching request %s: Status = %s, Duration = %d",
                        context.getLookupRequestId(), context.getDnbCode().getMessage(), context.getDuration()));
                break;
            }
            dnBAuthenticationService.refreshToken(DnBKeyType.REALTIME);
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
            NameLocation matchedNameLocation = new NameLocation();
            matchedNameLocation.setName((String) retrieveJsonValueFromResponse(entityNameJsonPath, response));
            matchedNameLocation.setStreet((String) retrieveJsonValueFromResponse(entityStreetJsonPath, response));
            matchedNameLocation.setCity((String) retrieveJsonValueFromResponse(entityCityJsonPath, response));
            matchedNameLocation.setState((String) retrieveJsonValueFromResponse(entityStateJsonPath, response));
            matchedNameLocation
                    .setCountryCode((String) retrieveJsonValueFromResponse(entityCountryCodeJsonPath, response));
            matchedNameLocation.setZipcode((String) retrieveJsonValueFromResponse(entityZipCodeJsonPath, response));
            matchedNameLocation
                    .setPhoneNumber((String) retrieveJsonValueFromResponse(entityPhoneNumberJsonPath, response));
            context.setMatchedNameLocation(matchedNameLocation);
            break;
        case REALTIME_EMAIL:
            context.setDuns((String) retrieveJsonValueFromResponse(emailDunsJsonPath, response));
            break;
        default:
            throw new LedpException(LedpCode.LEDP_25025, new String[] { apiType.name() });
        }
        if (!StringUtils.isEmpty(context.getDuns())) {
            context.setDnbCode(DnBReturnCode.OK);
        } else {
            log.warn(String.format("Fail to extract duns from response of request %: %", context.getLookupRequestId(),
                    response));
            context.setDnbCode(DnBReturnCode.BAD_RESPONSE);
        }

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
            if (ledpEx.getCode() == LedpCode.LEDP_25027) {
                context.setDnbCode(DnBReturnCode.EXPIRED_TOKEN);
            } else {
                context.setDnbCode(DnBReturnCode.BAD_REQUEST);
            }
        } else {
            log.warn("Unhandled exception in DnB realtime request " + context.getLookupRequestId() + ": "
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
                if (context.getInputNameLocation().getCountryCode().equals("DE")) {
                    url.append("OrderReasonCode=");
                    url.append(reasonCodeDe);
                    url.append("&");
                }
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
            if (StringUtils.isNotEmpty(context.getInputNameLocation().getZipcode())) {
                url.append("FullPostalCode=");
                url.append(context.getInputNameLocation().getZipcode());
                url.append("&");
            }
            if (StringUtils.isNotEmpty(context.getInputNameLocation().getPhoneNumber())) {
                url.append("TelephoneNumber=");
                url.append(context.getInputNameLocation().getPhoneNumber());
                url.append("&");
            }
            url.append(
                    "cleansematch=true&ConfidenceLowerLevelThresholdValue=1&IncludeCleansedAndStandardizedInformationIndicator=true");
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
