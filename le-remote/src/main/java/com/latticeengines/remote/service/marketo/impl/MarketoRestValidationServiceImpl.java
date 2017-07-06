package com.latticeengines.remote.service.marketo.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.remote.exposed.service.marketo.MarketoRestValidationService;

@Component("marketoRestValidationService")
public class MarketoRestValidationServiceImpl implements MarketoRestValidationService {

    @Override
    public boolean validateMarketoRestCredentials(String identityEndPoint, String restEndPoint, String clientId,
            String clientSecret) {
        String token = getToken(identityEndPoint, clientId, clientSecret);
        return describeLeads(token, restEndPoint);
    }

    private boolean describeLeads(String token, String restEndPoint) {
        boolean result = false;
        String fullRestEndPoint = restEndPoint + "/v1/leads/describe.json?access_token=" + token;
        try {
            URL url = new URL(fullRestEndPoint);
            HttpsURLConnection urlConn = (HttpsURLConnection) url.openConnection();
            urlConn.setRequestMethod("GET");
            urlConn.setRequestProperty("accept", "text/json");
            int responseCode = urlConn.getResponseCode();
            if (responseCode == 200) {
                InputStream inStream = urlConn.getInputStream();
                Reader reader = new InputStreamReader(inStream);
                ObjectMapper mapper = new ObjectMapper();
                JsonNode root = mapper.readTree(reader);
                result = root.get("success").asBoolean();
            } else {
                throw new LedpException(LedpCode.LEDP_21033, new String[] { restEndPoint });
            }
            urlConn.disconnect();
        } catch (MalformedURLException e) {
            throw new LedpException(LedpCode.LEDP_21031, new String[] { restEndPoint });
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_21032, new String[] { fullRestEndPoint });
        }
        return result;
    }

    private String getToken(String identityEndpoint, String clientId, String clientSecret) {
        String fullIdentityEndpoint = identityEndpoint + "/oauth/token?grant_type=client_credentials&client_id="
                + clientId + "&client_secret=" + clientSecret;

        String token = null;

        try {
            URL url = new URL(fullIdentityEndpoint);
            HttpsURLConnection urlConn = (HttpsURLConnection) url.openConnection();
            urlConn.setRequestMethod("GET");
            urlConn.setRequestProperty("accept", "application/json");
            int responseCode = urlConn.getResponseCode();
            if (responseCode == 200) {
                InputStream inStream = urlConn.getInputStream();
                Reader reader = new InputStreamReader(inStream);
                ObjectMapper mapper = new ObjectMapper();
                Map<String, String> results = mapper.readValue(reader, new TypeReference<Map<String, String>>() {
                });

                token = results.get("access_token");
            } else {
                throw new LedpException(LedpCode.LEDP_21030, new String[] { clientId, clientSecret });
            }
            urlConn.disconnect();
        } catch (MalformedURLException e) {
            throw new LedpException(LedpCode.LEDP_21028, new String[] { identityEndpoint });
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_21029, new String[] { fullIdentityEndpoint });
        }
        if (StringUtils.isBlank(token)) {
            throw new LedpException(LedpCode.LEDP_21030, new String[] { clientId, clientSecret });
        }
        return token;
    }

}