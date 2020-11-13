package com.latticeengines.datacloud.match.service.impl;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;

import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.datacloud.match.service.DnBRealTimeLookupService;
import com.latticeengines.datacloud.match.util.Direct2Utils;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBAPIType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("dnbRealTimeLookupService")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class DnBRealTimeLookupServiceImpl extends BaseDnBLookupServiceImpl<DnBMatchContext>
        implements DnBRealTimeLookupService {
    private static final Logger log = LoggerFactory.getLogger(DnBRealTimeLookupServiceImpl.class);

    @Inject
    private DnBAuthenticationServiceImpl dnBAuthenticationService;

    @Inject
    private DnBRealTimeLookupServiceImpl _self;

    @Value("${datacloud.dnb.realtime.url.prefix}")
    private String realTimeUrlPrefix;

    @Value("${datacloud.dnb.realtime.email.lookup.url.format}")
    private String emailLookupUrlFormat;

    @Value("${datacloud.dnb.authorization.header}")
    private String authorizationHeader;

    @Value("${datacloud.dnb.realtime.email.duns.jsonpath}")
    private String emailDunsJsonPath;

    @Value("${datacloud.dnb.retry.maxattempts}")
    private int retries;

    @Value("${datacloud.dnb.realtime.reasoncode.de}")
    private String reasonCodeDe;

    @Value("${datacloud.dnb.realtime.operatingstatus.outofbusiness}")
    private String outOfBusinessValue;

    @Value("${datacloud.dnb.realtime.errorcode.jsonpath}")
    private String errorCodeXpath;

    @Override
    protected DnBRealTimeLookupServiceImpl self() {
        return _self;
    }

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
            log.info("Attempting to refresh DnB token which was found invalid: " + context.getToken());
            dnBAuthenticationService.requestToken(DnBKeyType.REALTIME, context.getToken());
            if (i == retries - 1) {
                log.error("Fail to call dnb realtime email API due to invalid token and failed to refresh");
            }
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
            log.info("Attempting to refresh DnB token which was found invalid: " + context.getToken());
            dnBAuthenticationService.requestToken(DnBKeyType.REALTIME, context.getToken());
            if (i == retries - 1) {
                log.error("Fail to call dnb realtime API due to invalid token and failed to refresh");
            }
        }
        return context;
    }

    @Override
    protected void parseResponse(String response, DnBMatchContext context, DnBAPIType apiType) {
        switch (apiType) {
        case REALTIME_ENTITY:
            Direct2Utils.parseJsonResponse(response, context, apiType);
            if (CollectionUtils.isNotEmpty(context.getCandidates())) {
                DnBMatchCandidate topCandidate = context.getCandidates().get(0);
                context.setDuns(topCandidate.getDuns());
                context.setOrigDuns(topCandidate.getDuns());
                context.setConfidenceCode(topCandidate.getMatchInsight().getConfidenceCode());
                context.setMatchInsight(topCandidate.getMatchInsight());
                context.setMatchGrade(topCandidate.getMatchInsight().getMatchGrade());
                context.setMatchedNameLocation(topCandidate.getNameLocation());
                context.setOutOfBusiness(outOfBusinessValue.equalsIgnoreCase(topCandidate.getOperatingStatus()));
            }
            break;
        case REALTIME_EMAIL:
            String duns = (String) retrieveJsonValueFromResponse(emailDunsJsonPath, response, false);
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
    protected void parseError(Exception ex, DnBMatchContext context) {
        if (ex instanceof HttpClientErrorException) {
            HttpClientErrorException httpEx = (HttpClientErrorException) ex;
            log.error(String.format("HttpClientErrorException in DnB realtime request%s: HttpStatus %d %s",
                    context.getRootOperationUid() == null ? ""
                            : " (RootOperationID=" + context.getRootOperationUid() + ")",
                    ((HttpClientErrorException) ex).getStatusCode().value(),
                    ((HttpClientErrorException) ex).getStatusCode().name()));
            context.setDnbCode(parseDnBHttpError(httpEx));
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
        return new HttpEntity<>("", headers);
    }

    @Override
    protected String constructUrl(DnBMatchContext context, DnBAPIType apiType) {
        switch (apiType) {
        case REALTIME_ENTITY:
            StringBuilder url = new StringBuilder();
            url.append(realTimeUrlPrefix);
            if (!StringUtils.isEmpty(context.getInputNameLocation().getName())) {
                url.append("SubjectName=");
                url.append(urlEncode(context.getInputNameLocation().getName()));
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
                url.append(urlEncode(context.getInputNameLocation().getCity()));
                url.append("&");
            }
            if (!StringUtils.isEmpty(context.getInputNameLocation().getState())) {
                url.append("TerritoryName=");
                url.append(urlEncode(LocationUtils.getStardardStateCode(context.getInputNameLocation().getCountry(),
                        context.getInputNameLocation().getState())));
                url.append("&");
            }
            if (StringUtils.isNotEmpty(context.getInputNameLocation().getZipcode())) {
                url.append("FullPostalCode=");
                url.append(urlEncode(context.getInputNameLocation().getZipcode()));
                url.append("&");
            }
            if (StringUtils.isNotEmpty(context.getInputNameLocation().getPhoneNumber())) {
                url.append("TelephoneNumber=");
                url.append(urlEncode(context.getInputNameLocation().getPhoneNumber()));
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
    protected ResponseType getResponseType() {
        return ResponseType.JSON;
    }

    @Override
    protected String getErrorCodePath() {
        return errorCodeXpath;
    }

    @Override
    protected void updateTokenInContext(DnBMatchContext context, String token) {
        context.setToken(token);
    }

    private static String urlEncode(String val) {
        try {
            return URLEncoder.encode(val, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

}
