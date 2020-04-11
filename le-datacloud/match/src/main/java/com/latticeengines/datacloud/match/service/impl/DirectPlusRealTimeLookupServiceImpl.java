package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.datacloud.match.service.impl.BaseDnBLookupServiceImpl.ResponseType.JSON;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;

import com.latticeengines.datacloud.match.service.DnBRealTimeLookupService;
import com.latticeengines.datacloud.match.util.DirectPlusUtils;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBAPIType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("directPlusRealTimeLookupService")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class DirectPlusRealTimeLookupServiceImpl extends BaseDnBLookupServiceImpl<DnBMatchContext>
        implements DnBRealTimeLookupService {
    private static final Logger log = LoggerFactory.getLogger(DirectPlusRealTimeLookupServiceImpl.class);

    @Inject
    private DnBAuthenticationServiceImpl dnBAuthenticationService;

    @Inject
    private DirectPlusRealTimeLookupServiceImpl _self;

    @Value("${datacloud.dnb.direct.plus.lookup.url}")
    private String directPlusLookupUrl;

    @Value("${datacloud.dnb.retry.maxattempts}")
    private int retries;

    @Value("${datacloud.dnb.realtime.operatingstatus.outofbusiness}")
    private String outOfBusinessValue;

    @Value("${datacloud.dnb.direct.plus.errorcode.jsonpath}")
    private String errorCodeXpath;

    @Override
    protected DirectPlusRealTimeLookupServiceImpl self() {
        return _self;
    }

    @Override
    public DnBMatchContext realtimeEntityLookup(DnBMatchContext context) {
        for (int i = 0; i < retries; i++) {
            long startTime = System.currentTimeMillis();
            executeLookup(context, DnBKeyType.DPLUS, DnBAPIType.REALTIME_ENTITY);
            context.setDuration(System.currentTimeMillis() - startTime);
            if (context.getDnbCode() != DnBReturnCode.UNAUTHORIZED) {
                log.info(String.format("Direct+ transaction matching request %s%s: Status=%s, Duration=%d",
                        context.getLookupRequestId(),
                        context.getRootOperationUid() == null ? ""
                                : " (RootOperationID=" + context.getRootOperationUid() + ")",
                        context.getDnbCode(), context.getDuration()));
                break;
            }
            log.info("Attempting to refresh DnB token which was found invalid: " + context.getToken());
            dnBAuthenticationService.requestToken(DnBKeyType.DPLUS, context.getToken());
            if (i == retries - 1) {
                log.error("Fail to call dnb realtime email API due to invalid token and failed to refresh");
            }
        }
        return context;
    }

    @Override
    public DnBMatchContext realtimeEmailLookup(DnBMatchContext context) {
        throw new UnsupportedOperationException("Real time email lookup via Direct+ is not supported yet.");
    }

    @Override
    protected void parseResponse(String response, DnBMatchContext context, DnBAPIType apiType) {
        switch (apiType) {
            case REALTIME_ENTITY:
            case REALTIME_EMAIL:
                DirectPlusUtils.parseJsonResponse(response, context, apiType);
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
                if (JSON.equals(getResponseType())) {
                    if (ex.getCause() instanceof HttpClientErrorException) {
                        String response = ((HttpClientErrorException) ex.getCause()).getResponseBodyAsString();
                        String errorCode = (String) retrieveJsonValueFromResponse(getErrorCodePath(), response, false);
                        if ("20504".equals(errorCode)) {
                            // Invalid Territory
                            context.setDnbCode(DnBReturnCode.UNMATCH);
                            break;
                        }
                    }
                }
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
        headers.add(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        return new HttpEntity<>("", headers);
    }

    @Override
    protected String constructUrl(DnBMatchContext context, DnBAPIType apiType) {
        return directPlusLookupUrl + "?" + DirectPlusUtils.constructUrlParams(context, apiType);
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


}
