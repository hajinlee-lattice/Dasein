package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.jayway.jsonpath.JsonPath;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBAPIType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.proxy.exposed.RestApiClient;

public abstract class BaseDnBLookupServiceImpl<T> {
    private static final Log log = LogFactory.getLog(BaseDnBLookupServiceImpl.class);

    @Autowired
    private DnBAuthenticationServiceImpl dnBAuthenticationService;

    private RestApiClient dnbClient = new RestApiClient();

    @Value("${datacloud.dnb.realtime.resultid.jsonpath}")
    private String resultIdJsonPath;

    protected abstract String constructUrl(T context, DnBAPIType apiType);

    protected abstract HttpEntity<String> constructEntity(T context, String token);

    protected abstract void parseResponse(String response, T context, DnBAPIType apiType);

    protected abstract void parseError(Exception ex, T context);
    
    public void executeLookup(T context, DnBKeyType keyType, DnBAPIType apiType) {
        try {
            String token = dnBAuthenticationService.requestToken(keyType);
            String url = constructUrl(context, apiType);
            HttpEntity<String> entity = constructEntity(context, token);
            String response = sendRequest(url, entity, apiType);
            parseResponse(response, context, apiType);
        } catch (Exception ex) {
            parseError(ex, context);
        }
    }

    protected String sendRequest(String url, HttpEntity<String> entity, DnBAPIType apiType) {
        if (apiType == DnBAPIType.REALTIME_ENTITY || apiType == DnBAPIType.REALTIME_EMAIL
                || apiType == DnBAPIType.BATCH_FETCH) {
            return dnbClient.get(entity, url);
        } else {
            return dnbClient.post(entity, url);
        }
    }

    protected DnBReturnCode parseDnBHttpError(HttpClientErrorException ex) {
        switch (ex.getStatusCode()) {
        case REQUEST_TIMEOUT:
            return DnBReturnCode.TIMEOUT;
        case BAD_REQUEST:
            return DnBReturnCode.BAD_REQUEST;
        case NOT_FOUND:
            return DnBReturnCode.UNMATCH;
        default:
            return parseErrorBody(ex.getResponseBodyAsString());
        }
    }

    @SuppressWarnings("unchecked")
    protected DnBReturnCode parseErrorBody(String body) {
        try {
            List<String> code = (List<String>) retrieveJsonValueFromResponse(resultIdJsonPath, body, true);
            String dnBErrorCode = code.get(0);
            switch (dnBErrorCode) {
            case "SC001":
                return DnBReturnCode.UNAUTHORIZED;
            case "SC002":
                return DnBReturnCode.UNAUTHORIZED;
            case "SC003":
                return DnBReturnCode.EXPIRED_TOKEN;
            case "SC004":
                return DnBReturnCode.EXPIRED_TOKEN;
            case "SC005":
                return DnBReturnCode.EXCEED_REQUEST_NUM;
            case "SC006":
                return DnBReturnCode.EXCEED_CONCURRENT_NUM;
            case "SC007":
                return DnBReturnCode.UNAUTHORIZED;
            case "SC008":
                return DnBReturnCode.UNAUTHORIZED;
            case "SC009":
                return DnBReturnCode.UNAUTHORIZED;
            case "SC012":
                return DnBReturnCode.UNAUTHORIZED;
            case "SC014":
                return DnBReturnCode.UNAUTHORIZED;
            default:
                return DnBReturnCode.UNKNOWN;
            }
        } catch (Exception ex) {
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
            log.error(e);
        }

        return result;
    }
}
