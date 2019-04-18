package com.latticeengines.remote.service.marketo.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.HttpClientUtils;
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
        RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
        String fullRestEndPoint = restEndPoint + "/v1/leads/describe.json?access_token=" + token;
        try {
            ResponseEntity<String> response = restTemplate.getForEntity(fullRestEndPoint,
                    String.class);
            if(response.getStatusCode() != HttpStatus.OK) {
                throw new LedpException(LedpCode.LEDP_21033, new String[] { restEndPoint });
            }

            String responseString = response.getBody();
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(responseString);
            result = root.get("success").asBoolean();
        } catch (ResourceAccessException rae) {
            throw new LedpException(LedpCode.LEDP_21031, rae, new String[] { restEndPoint });
        } catch (RestClientException rce) {
            throw new LedpException(LedpCode.LEDP_21032, rce, new String[] { restEndPoint });
        } catch (IOException ioe) {
            throw new LedpException(LedpCode.LEDP_21033, ioe, new String[] { restEndPoint });
        }
        return result;
    }

    @SuppressWarnings("rawtypes")
    private String getToken(String identityEndpoint, String clientId, String clientSecret) {
        RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
        Object accessToken = null;
        try {
            Map<String, String> creds = new HashMap<>();
            creds.put("clientId", clientId);
            creds.put("clientSecret", clientSecret);
            ResponseEntity<Map> response = restTemplate.getForEntity(identityEndpoint
                    + "/oauth/token?grant_type=client_credentials&client_id={clientId}&client_secret={clientSecret}",
                    Map.class, creds);
            if(response.getStatusCode() != HttpStatus.OK) {
                throw new LedpException(LedpCode.LEDP_21030, new String[] { clientId, clientSecret });
            }
            @SuppressWarnings("unchecked")
            Map<String, String> tokens = (Map<String, String>) response.getBody();
            accessToken = tokens.get("access_token");
        } catch(ResourceAccessException rae) {
            throw new LedpException(LedpCode.LEDP_21028, new String[] { rae.getMessage() });
        } catch (HttpClientErrorException hce) {
            throw new LedpException(LedpCode.LEDP_21030, new String[] { hce.getMessage() });
        } catch(RestClientException rce) {
            throw new LedpException(LedpCode.LEDP_21029, new String[] { rce.getMessage() });
        }

        if (StringUtils.isBlank(accessToken.toString())) {
            throw new LedpException(LedpCode.LEDP_21030, new String[] { clientId, clientSecret });
        }
        return accessToken.toString();
    }

}
