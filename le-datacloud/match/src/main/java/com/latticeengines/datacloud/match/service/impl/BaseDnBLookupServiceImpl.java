package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.io.StringReader;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.jayway.jsonpath.JsonPath;
import com.latticeengines.datacloud.match.exposed.service.DnBAuthenticationService;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBAPIType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.proxy.exposed.RestApiClient;

/*
 * Base class for the service sending lookup requests to a D&B API (e.g. Direct+)
 */
public abstract class BaseDnBLookupServiceImpl<T> {
    private static final Logger log = LoggerFactory.getLogger(BaseDnBLookupServiceImpl.class);

    @Inject
    private DnBAuthenticationService dnbAuthenticationService;

    private RestApiClient dnbClient;

    protected abstract String constructUrl(T context, DnBAPIType apiType);

    protected abstract HttpEntity<String> constructEntity(T context, String token);

    /*
     * Parse a 200 response from the D&B lookup
     * @param response - response from lookup
     * @param context - context for the given match
     * @param apiType
     */
    protected abstract void parseResponse(String response, T context, DnBAPIType apiType);

    protected abstract void parseError(Exception ex, T context);

    protected abstract String getErrorCodePath();

    protected abstract ResponseType getResponseType();

    protected abstract void updateTokenInContext(T context, String token);

    protected abstract BaseDnBLookupServiceImpl<T> self();

    @Inject
    private ApplicationContext applicationContext;

    @PostConstruct
    public void initialize() {
        dnbClient = RestApiClient.newExternalClient(applicationContext);
        dnbClient.setErrorHandler(new GetDnBResponseErrorHandler());
        dnbClient.setUseUri(true);
    }

    void executeLookup(T context, DnBKeyType keyType, DnBAPIType apiType) {
        try {
            String url = constructUrl(context, apiType);
            String token = dnbAuthenticationService.requestToken(keyType, null);
            updateTokenInContext(context, token);
            HttpEntity<String> entity = constructEntity(context, token);
            if (keyType == DnBKeyType.BATCH) {
                log.info("Submitting request {} with token {}", url, token);
            }
            String response = sendRequest(url, entity, apiType);
            parseResponse(response, context, apiType);
        } catch (Exception ex) {
            parseError(ex, context);
        }
    }

    private String sendRequest(String url, HttpEntity<String> entity, DnBAPIType apiType) {
        if (apiType == DnBAPIType.REALTIME_ENTITY || apiType == DnBAPIType.REALTIME_EMAIL) {
            return dnbClient.get(entity, url);
        } else if (DnBAPIType.BATCH_FETCH.equals(apiType) || DnBAPIType.BATCH_STATUS.equals(apiType)) {
            return dnbClient.get(entity, url);
        } else {
            return dnbClient.post(entity, url);
        }
    }

    @Cacheable(cacheNames = CacheName.Constants.DnBRealTimeLookup, key = "T(java.lang.String).format(\"%s\", #url)")
    public String sendCacheableRequest(String url, HttpEntity<String> entity) {
        return dnbClient.get(entity, url);
    }

    /**
     * Convert an error response from D&B lookup service to a local enum
     * @param ex - HttpClientErrorException
     * @return - corresponding enum for the given status code and embedded status code
     */
    DnBReturnCode parseDnBHttpError(HttpClientErrorException ex) {
        String response = ex.getResponseBodyAsString();
        switch (ex.getStatusCode()) {
        case REQUEST_TIMEOUT:
            return DnBReturnCode.TIMEOUT;
        case UNAUTHORIZED:
        case FORBIDDEN:
            String errorCode = null;
            switch (getResponseType()) {
            case XML:
                errorCode = (String) retrieveXmlValueFromResponse(getErrorCodePath(), response);
                break;
            case JSON:
                errorCode = (String) retrieveJsonValueFromResponse(getErrorCodePath(), response, false);
                break;
            case CSV:
                // no way to parse http error from CSV
                break;
            default:
                throw new UnsupportedOperationException("Unknown response type " + getResponseType());
            }
            if (errorCode == null) {
                log.error("Fail to parse DnB error code from response");
                return DnBReturnCode.UNKNOWN;
            }
            switch (errorCode) {
            case "SC001":
            case "SC002":
            case "SC003":
            case "SC004":
            case "00001":
            case "00002":
            case "00003":
            case "00004":
                return DnBReturnCode.UNAUTHORIZED;
            case "SC005":
            case "SC006":
            case "00005":
            case "00006":
                return DnBReturnCode.RATE_LIMITING;
            default:
                return DnBReturnCode.UNKNOWN;
            }
        default:
            return DnBReturnCode.UNKNOWN;
        }
    }

    protected Object retrieveJsonValueFromResponse(String jsonPath, String body, boolean fieldRequired) {
        try {
            return JsonPath.parse(body).read(jsonPath);
        } catch (Exception e) {
            if (fieldRequired) {
                throw e;
            } else {
                log.warn(String.format("Optional field (json path %s) not exist in response %s", jsonPath, body), e);
                return null;
            }
        }

    }

    private void disallowDTDs_XXE(DocumentBuilderFactory dbf)
            throws ParserConfigurationException, SAXException, IOException {
        String FEATURE = null;
        // This is the PRIMARY defense. If DTDs (doctypes) are disallowed, almost all
        // XML entity attacks are prevented
        // Xerces 2 only -
        // http://xerces.apache.org/xerces2-j/features.html#disallow-doctype-decl
        FEATURE = "http://apache.org/xml/features/disallow-doctype-decl";
        dbf.setFeature(FEATURE, true);

        // If you can't completely disable DTDs, then at least do the following:
        // Xerces 1 -
        // http://xerces.apache.org/xerces-j/features.html#external-general-entities
        // Xerces 2 -
        // http://xerces.apache.org/xerces2-j/features.html#external-general-entities
        // JDK7+ - http://xml.org/sax/features/external-general-entities
        // This feature has to be used together with the following one, otherwise it
        // will not protect you from XXE for sure
        FEATURE = "http://xml.org/sax/features/external-general-entities";
        dbf.setFeature(FEATURE, false);

        // Xerces 1 -
        // http://xerces.apache.org/xerces-j/features.html#external-parameter-entities
        // Xerces 2 -
        // http://xerces.apache.org/xerces2-j/features.html#external-parameter-entities
        // JDK7+ - http://xml.org/sax/features/external-parameter-entities
        // This feature has to be used together with the previous one, otherwise it will
        // not protect you from XXE for sure
        FEATURE = "http://xml.org/sax/features/external-parameter-entities";
        dbf.setFeature(FEATURE, false);

        // Disable external DTDs as well
        FEATURE = "http://apache.org/xml/features/nonvalidating/load-external-dtd";
        dbf.setFeature(FEATURE, false);

        // and these as well, per Timothy Morgan's 2014 paper: "XML Schema, DTD, and
        // Entity Attacks"
        dbf.setXIncludeAware(false);
        dbf.setExpandEntityReferences(false);

        // And, per Timothy Morgan: "If for some reason support for inline DOCTYPEs are
        // a requirement, then
        // ensure the entity settings are disabled (as shown above) and beware that SSRF
        // attacks
        // (http://cwe.mitre.org/data/definitions/918.html) and denial
        // of service attacks (such as billion laughs or decompression bombs via "jar:")
        // are a risk."
    }

    protected Object retrieveXmlValueFromResponse(String path, String response) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;
        String result = "";
        try {
            disallowDTDs_XXE(factory);
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

    public enum ResponseType {
        XML, JSON, CSV
    }
}
