package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.io.StringReader;

import javax.annotation.PostConstruct;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.jayway.jsonpath.JsonPath;
import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBAPIType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.proxy.exposed.RestApiClient;

public abstract class BaseDnBLookupServiceImpl<T> {
    private static final Logger log = LoggerFactory.getLogger(BaseDnBLookupServiceImpl.class);

    @Autowired
    private DnBAuthenticationServiceImpl dnBAuthenticationService;

    private RestApiClient dnbClient;

    @Value("${datacloud.dnb.realtime.resultid.jsonpath}")
    private String resultIdJsonPath;

    protected abstract String constructUrl(T context, DnBAPIType apiType);

    protected abstract HttpEntity<String> constructEntity(T context, String token);

    protected abstract void parseResponse(String response, T context, DnBAPIType apiType);

    protected abstract void parseError(String response, Exception ex, T context);

    protected abstract String getResultIdPath();

    @Autowired
    private ApplicationContext applicationContext;

    @PostConstruct
    public void initialize() {
        dnbClient = RestApiClient.newExternalClient(applicationContext);
        dnbClient.setErrorHandler(new GetDnBResponseErrorHandler());
    }

    public void executeLookup(T context, DnBKeyType keyType, DnBAPIType apiType) {
        String response = null;
        try {
            String token = dnBAuthenticationService.requestToken(keyType);
            String url = constructUrl(context, apiType);
            HttpEntity<String> entity = constructEntity(context, token);
            if (keyType == DnBKeyType.BATCH) {
                log.info("Submitting request {} with token {}", url, token);
            }
            response = sendRequest(url, entity, apiType);
            parseResponse(response, context, apiType);
        } catch (Exception ex) {
            parseError(response, ex, context);
        }
    }

    protected String sendRequest(String url, HttpEntity<String> entity, DnBAPIType apiType) {
        if (apiType == DnBAPIType.REALTIME_ENTITY || apiType == DnBAPIType.REALTIME_EMAIL
                || apiType == DnBAPIType.BATCH_FETCH || apiType == DnBAPIType.BATCH_STATUS) {
            return dnbClient.get(entity, url);
        } else {
            return dnbClient.post(entity, url);
        }
    }

    protected DnBReturnCode parseDnBHttpError(String response, HttpClientErrorException ex) {
        switch (ex.getStatusCode()) {
        case REQUEST_TIMEOUT:
            return DnBReturnCode.TIMEOUT;
        case UNAUTHORIZED:
            String resultId = (String) retrieveXmlValueFromResponse(getResultIdPath(), response);
            switch (resultId) {
            case "SC001":
            case "SC002":
            case "SC003":
            case "SC004":
                return DnBReturnCode.UNAUTHORIZED;
            case "SC005":
            case "SC006":
                return DnBReturnCode.RATE_LIMITING;
            default:
                return DnBReturnCode.UNKNOWN;
            }
        default:
            return DnBReturnCode.UNKNOWN;
        }
    }

    protected Object retrieveJsonValueFromResponse(String jsonPath, String body, boolean raiseException) {
        try {
            return JsonPath.parse(body).read(jsonPath);
        } catch (Exception e) {
            if (raiseException) {
                throw e;
            } else {
                return null;
            }
        }

    }

    protected Object retrieveXmlValueFromResponse(String path, String response) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;
        String result = "";
        try {
            builder = factory.newDocumentBuilder();
            Document document = builder.parse(new InputSource(new StringReader(response)));
            XPath xpath = XPathFactory.newInstance().newXPath();
            result = (String) xpath.evaluate(path, document, XPathConstants.STRING);
        } catch (ParserConfigurationException | IOException | XPathExpressionException | SAXException e) {
            log.error(e.getMessage(), e);
        }

        return result;
    }

    protected void logRateLimitingRejection(RateLimitedAcquisition rlAcq, DnBAPIType apiType) {
        StringBuilder sb1 = new StringBuilder();
        if (rlAcq.getRejectionReasons() != null) {
            for (String rejectionReason : rlAcq.getRejectionReasons()) {
                sb1.append(rejectionReason + " ");
            }
        }
        StringBuilder sb2 = new StringBuilder();
        if (rlAcq.getExceedingQuotas() != null) {
            for (String exceedingQuota : rlAcq.getExceedingQuotas()) {
                sb2.append(exceedingQuota + " ");
            }
        }
        switch (apiType) {
        case BATCH_DISPATCH:
            log.error("Fail to submit batched request. Rejection reasons: " + sb1.toString() + ". Exceeding quotas: "
                    + sb2.toString());
            break;
        case BATCH_STATUS:
            log.error("Fail to get status for batched requests. Rejection reasons: " + sb1.toString()
                    + ". Exceeding quotas: " + sb2.toString());
            break;
        default:
            break;
        }
    }
}
