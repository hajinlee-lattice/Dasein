package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.io.StringReader;

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
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.jayway.jsonpath.JsonPath;
import com.latticeengines.datacloud.match.dnb.DnBKeyType;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

public abstract class BaseDnBLookupServiceImpl<T> {
    private static final Log log = LogFactory.getLog(BaseDnBLookupServiceImpl.class);

    protected static final String DNB_RETURN_CODE = "DNB_RETURN_CODE";

    @Autowired
    private DnBAuthenticationServiceImpl dnBAuthenticationService;

    @Value("${datacloud.dnb.realtime.resultid.jsonpath}")
    private String resultIdJsonPath;

    public DataFlowContext executeLookup(T input, DnBKeyType keyType) {
        DataFlowContext context = new DataFlowContext();
        String token = dnBAuthenticationService.requestToken(keyType);
        String url = constructUrl(input);
        try {
            HttpEntity<String> entity = constructEntity(input, token);
            ResponseEntity<String> response = sendRequestToDnB(url, entity);
            parseSucceededResponse(response, context);
        } catch (HttpClientErrorException ex) {
            parseDnBHttpError(ex, context);
        }

        return context;
    }

    protected abstract HttpEntity<String> constructEntity(T input, String token);

    private void parseDnBHttpError(HttpClientErrorException ex, DataFlowContext context) {
        DnBReturnCode errCode = null;
        Boolean isNeedParseBody = false;
        switch (ex.getStatusCode()) {
        case REQUEST_TIMEOUT:
            errCode = DnBReturnCode.TIMEOUT;
            break;
        case NOT_FOUND:
            errCode = DnBReturnCode.NO_RESULT;
            break;
        case BAD_REQUEST:
            errCode = DnBReturnCode.BAD_REQUEST;
            break;
        default:
            isNeedParseBody = true;
        }
        if (isNeedParseBody) {
            errCode = parseErrorBody(ex.getResponseBodyAsString());
        }
        context.setProperty(DNB_RETURN_CODE, errCode);
    }

    private DnBReturnCode parseErrorBody(String body) {
        DnBReturnCode errCode;
        String dnBErrorCode = (String) retrieveJsonValueFromResponse(resultIdJsonPath, body);
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

    protected Object retrieveJsonValueFromResponse(String jsonPath, String body) {
        return JsonPath.parse(body).read(jsonPath);
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

    protected String normalizeString(String str) {
        if (str == null) {
            return "";
        }

        return str.trim();
    }

    protected abstract void parseSucceededResponse(ResponseEntity<String> response, DataFlowContext context);

    protected abstract ResponseEntity<String> sendRequestToDnB(String url, HttpEntity<String> entity);

    protected abstract String constructUrl(T input);
}
