package com.latticeengines.datacloud.match.service.impl;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;

import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.datacloud.match.service.DnBRealTimeLookupService;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBAPIType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("dnbRealTimeLookupService")
public class DnBRealTimeLookupServiceImpl extends BaseDnBLookupServiceImpl<DnBMatchContext>
        implements DnBRealTimeLookupService {
    private static final Logger log = LoggerFactory.getLogger(DnBRealTimeLookupServiceImpl.class);

    @Autowired
    private DnBAuthenticationServiceImpl dnBAuthenticationService;

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

    @Value("${datacloud.dnb.retry.maxattempts}")
    private int retries;

    @Value("${datacloud.dnb.realtime.reasoncode.de}")
    private String reasonCodeDe;

    @Value("${datacloud.dnb.realtime.operatingstatus.jsonpath}")
    private String operatingStatusJsonPath;

    @Value("${datacloud.dnb.realtime.operatingstatus.outofbusiness}")
    private String outOfBusinessValue;

    @Value("${datacloud.dnb.bulk.getstatus.transactioncode.xpath}")
    private String transactionCodeXPath;

    @Override
    public DnBMatchContext realtimeEntityLookup(DnBMatchContext context) {
        for (int i = 0; i < retries; i++) {
            Long startTime = System.currentTimeMillis();
            executeLookup(context, DnBKeyType.REALTIME, DnBAPIType.REALTIME_ENTITY);
            context.setDuration(System.currentTimeMillis() - startTime);
            if (context.getDnbCode() != DnBReturnCode.UNAUTHORIZED) {
                log.info(String.format("DnB realtime entity matching request %s%s: Status=%s, Duration=%d",
                        context.getLookupRequestId(),
                        context.getRootOperationUid() == null ? ""
                                : " (RootOperationID=" + context.getRootOperationUid() + ")",
                        context.getDnbCode(), context.getDuration()));
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
            if (context.getDnbCode() != DnBReturnCode.UNAUTHORIZED || i == retries - 1) {
                log.info(String.format("DnB realtime email matching request %s%s: Status=%s, Duration=%d",
                        context.getLookupRequestId(),
                        context.getRootOperationUid() == null ? ""
                                : " (RootOperationID=" + context.getRootOperationUid() + ")",
                        context.getDnbCode(), context.getDuration()));
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
            String duns = (String) retrieveJsonValueFromResponse(entityDunsJsonPath, response, false);
            context.setDuns(duns);
            context.setOrigDuns(duns);
            context.setConfidenceCode(
                    (Integer) retrieveJsonValueFromResponse(entityConfidenceCodeJsonPath, response, false));
            context.setMatchGrade((String) retrieveJsonValueFromResponse(entityMatchGradeJsonPath, response, false));
            if (context.getDuns() == null || context.getConfidenceCode() == null || context.getMatchGrade() == null) {
                context.setDnbCode(DnBReturnCode.UNMATCH);
                return;
            }
            NameLocation matchedNameLocation = context.getMatchedNameLocation();
            matchedNameLocation.setName((String) retrieveJsonValueFromResponse(entityNameJsonPath, response, false));
            matchedNameLocation
                    .setStreet((String) retrieveJsonValueFromResponse(entityStreetJsonPath, response, false));
            matchedNameLocation.setCity((String) retrieveJsonValueFromResponse(entityCityJsonPath, response, false));
            matchedNameLocation.setState((String) retrieveJsonValueFromResponse(entityStateJsonPath, response, false));
            matchedNameLocation
                    .setCountryCode((String) retrieveJsonValueFromResponse(entityCountryCodeJsonPath, response, false));
            matchedNameLocation
                    .setZipcode((String) retrieveJsonValueFromResponse(entityZipCodeJsonPath, response, false));
            matchedNameLocation
                    .setPhoneNumber((String) retrieveJsonValueFromResponse(entityPhoneNumberJsonPath, response, false));
            String outOfBusiness = (String) retrieveJsonValueFromResponse(operatingStatusJsonPath, response, false);
            if (outOfBusinessValue.equalsIgnoreCase(outOfBusiness)) {
                context.setOutOfBusiness(Boolean.TRUE);
            } else {
                context.setOutOfBusiness(Boolean.FALSE);
            }
            break;
        case REALTIME_EMAIL:
             duns = (String) retrieveJsonValueFromResponse(emailDunsJsonPath, response, false);
            context.setDuns(duns);
            context.setOrigDuns(duns);
            if (context.getDuns() == null) {
                context.setDnbCode(DnBReturnCode.UNMATCH);
                return;
            }
            break;
        default:
            throw new LedpException(LedpCode.LEDP_25025, new String[] { apiType.name() });
        }
        context.setDnbCode(DnBReturnCode.OK);
    }

    @Override
    protected void parseError(String response, Exception ex, DnBMatchContext context) {
        if (ex instanceof HttpClientErrorException) {
            HttpClientErrorException httpEx = (HttpClientErrorException) ex;
            log.error(String.format("HttpClientErrorException in DnB realtime request%s: HttpStatus %d %s",
                    context.getRootOperationUid() == null ? ""
                            : " (RootOperationID=" + context.getRootOperationUid() + ")",
                    ((HttpClientErrorException) ex).getStatusCode().value(),
                    ((HttpClientErrorException) ex).getStatusCode().name()));
            context.setDnbCode(parseDnBHttpError(response, httpEx));
        } else if (ex instanceof LedpException) {
            LedpException ledpEx = (LedpException) ex;
            // If DnB cannot find duns for match input, HttpStatus.NOT_FOUND (LedpCode.LEDP_25038) is returned. 
            // Treat LedpCode.LEDP_25038 as normal response. Do not log
            if (ledpEx.getCode() != LedpCode.LEDP_25038) {
                log.error(String.format("LedpException in DnB realtime request%s: %s %s",
                        context.getRootOperationUid() == null ? ""
                                : " (RootOperationID=" + context.getRootOperationUid() + ")",
                        ((LedpException) ex).getCode().name(), ((LedpException) ex).getCode().getMessage()));
            }
            switch (ledpEx.getCode()) {
            case LEDP_25027:
                context.setDnbCode(DnBReturnCode.UNAUTHORIZED);
                break;
            case LEDP_25037:
                context.setDnbCode(DnBReturnCode.BAD_REQUEST);
                break;
            case LEDP_25038:
                context.setDnbCode(DnBReturnCode.UNMATCH);
                break;
            case LEDP_25039:
                context.setDnbCode(DnBReturnCode.SERVICE_UNAVAILABLE);
                break;
            default:
                context.setDnbCode(DnBReturnCode.UNKNOWN);
                break;
            }
        } else {
            log.error(String.format("Unhandled exception in DnB realtime request %s%s", context.getLookupRequestId(),
                    context.getRootOperationUid() == null ? ""
                            : " (RootOperationID=" + context.getRootOperationUid() + ")"),
                    ex);
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
                url.append(LocationUtils.getStardardStateCode(context.getInputNameLocation().getCountry(),
                        context.getInputNameLocation().getState()));
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

    @Override
    protected String getResultIdPath() {
        return transactionCodeXPath;
    }
}
