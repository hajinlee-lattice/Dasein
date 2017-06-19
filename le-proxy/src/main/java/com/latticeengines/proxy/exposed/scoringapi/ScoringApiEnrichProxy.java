package com.latticeengines.proxy.exposed.scoringapi;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResponseErrorHandler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.RemoteLedpException;
import com.latticeengines.domain.exposed.scoringapi.EnrichRequest;
import com.latticeengines.domain.exposed.scoringapi.EnrichResponse;
import com.latticeengines.domain.exposed.scoringapi.EnrichResponseMetadata;
import com.latticeengines.network.exposed.scoringapi.ScoringApiEnrichInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("scoringApiEnrichProxy")
public class ScoringApiEnrichProxy extends BaseRestApiProxy implements ScoringApiEnrichInterface {

    private static class ScoringErrorHandler implements ResponseErrorHandler {
        @Override
        public boolean hasError(ClientHttpResponse response) throws IOException {
            return response.getStatusCode() != HttpStatus.OK;
        }

        @Override
        public void handleError(ClientHttpResponse response) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            IOUtils.copy(response.getBody(), baos);
            String body = new String(baos.toByteArray());
            try {
                @SuppressWarnings("unused")
                JsonNode node = new ObjectMapper().readTree(body);
            } catch (Exception e) {
                // Throw a non-LedpException to allow for retries
                throw new RuntimeException(
                        String.format("Received status code %s from scoring api (%s)", response.getStatusCode(), body));
            }
            throw new RemoteLedpException(null, response.getStatusCode(), LedpCode.LEDP_00002, body);
        }
    }

    public ScoringApiEnrichProxy() {
        super(PropertyUtils.getProperty("common.scoringapi.url"), "/score/enrich");
        setErrorHandler(new ScoringErrorHandler());
    }

    @Override
    public EnrichResponse enrichRecord(EnrichRequest request, String tenantIdentifier, String credentialId) {
        String uuid = UuidUtils.packUuid(tenantIdentifier, credentialId);
        String url = constructUrl("/record/{uuid}", uuid);
        @SuppressWarnings("unchecked")
        Map<String, ?> map = post("enrichRecord", url, request, Map.class);
        EnrichResponse response = new EnrichResponse();
        if (map.containsKey(EnrichResponse.ENRICH_RESPONSE_METADATA)) {
            EnrichResponseMetadata responseMetadata = JsonUtils
                    .convertValue(map.get(EnrichResponse.ENRICH_RESPONSE_METADATA), EnrichResponseMetadata.class);
            response.setResponseMetadata(responseMetadata);
        }

        Map<String, Object> enrichmentAttributeValues = new HashMap<>();
        for (String key : map.keySet()) {
            if (key.equals(EnrichResponse.ENRICH_RESPONSE_METADATA)) {
                continue;
            }
            enrichmentAttributeValues.put(key, map.get(key));
        }
        response.setEnrichmentAttributeValues(enrichmentAttributeValues);

        return response;
    }
}
