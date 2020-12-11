package com.latticeengines.security.service.impl;

import java.net.URI;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.domain.exposed.dcp.vbo.VboCallback;
import com.latticeengines.domain.exposed.dcp.vbo.VboUserSeatUsageEvent;
import com.latticeengines.security.service.AuthorizationServiceBase;
import com.latticeengines.security.service.VboService;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;


@Service("vboService")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class VboServiceImpl extends AuthorizationServiceBase implements VboService {
    private static Logger log = LoggerFactory.getLogger(VboServiceImpl.class);

    @Inject
    private VboServiceImpl _self;

    @Value("${security.vbo.usage.url}")
    private String usageEventUrl;

    @Override
    protected String refreshOAuthTokens(String cacheKey) {
        return _self.getTokenFromIDaaS(clientId);
    }

    @Override
    public void sendProvisioningCallback(VboCallback callback) {
        refreshToken();

        String url = callback.targetUrl;
        log.info("Sending callback to " + url);
        log.info(callback.toString());
        String traceId = callback.customerCreation.transactionDetail.ackRefId;

        try {
            ResponseEntity<String> response = restTemplate.postForEntity(URI.create(url), callback, String.class);
            log.info("Callback {} finished with response code {}", traceId, response.getStatusCodeValue());
            log.info("Callback {} response body: {}", traceId, response.getBody());
        } catch (Exception e) {
            log.error(traceId + " Exception in callback:" + e.toString());
            throw e;
        }
    }

    @Override
    public void sendUserUsageEvent(VboUserSeatUsageEvent usageEvent) {
        refreshToken();

        String logMsg = "Sending VBO User Seat Usage Event for user " + usageEvent.getEmailAddress();
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.activeSpan();
        if (span != null)
            span.log(logMsg);
        log.info(logMsg);

        try {
            ResponseEntity<String> response = restTemplate.postForEntity(URI.create(getUsageEventUrl()), usageEvent, String.class);
            log.info(response.getStatusCode().toString());
            log.info(response.getBody());
        } catch (Exception e) {
            log.error("Exception in usage event: " + e.toString());
            throw e;
        }
    }

    private String getUsageEventUrl() {
        return usageEventUrl + "/usage";
    }

    @Override
    public JsonNode getSubscriberMeter(String subscriberNumber) {
        refreshToken();
        String urlPattern = "/event/meter/";
        URI uri = URI.create(usageEventUrl + urlPattern + subscriberNumber);
        try {
            ResponseEntity<JsonNode> response = restTemplate.getForEntity(uri, JsonNode.class);
            if (response.getStatusCodeValue() != 200) {
                log.error(String.format("Retrieved status code %s while trying to get usage meter for subscriber %s",
                        response.getStatusCodeValue(), subscriberNumber));
                return null;
            }
            // return "meter" node from D&B Connect product's "STCT" domain add-on
            JsonNode n = getNodeFromList(response.getBody(), "products", "name", "D&B Connect");
            n = getNodeFromList(n, "domain_add_ons", "canonical_name", "STCT");
            return (n != null && n.has("meter")) ? n.get("meter") : null;
        } catch (Exception e) {
            log.error(String.format("Failed to get usage meter for subscriber %s:", subscriberNumber), e);
        }
        return null;
    }

    private JsonNode getNodeFromList(JsonNode node, String listField, String key, String value) {
        if (node == null) return null;
        if (node.has(listField) && node.get(listField).size() > 0) {
            for (JsonNode n : node.get(listField)) {
                if (n.get(key).asText().equals(value)) return n;
            }
        }
        return null;
    }
}
