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

import com.latticeengines.domain.exposed.dcp.vbo.VboCallback;
import com.latticeengines.domain.exposed.dcp.vbo.VboUserSeatUsageEvent;
import com.latticeengines.security.service.AuthorizationServiceBase;
import com.latticeengines.security.service.VboService;


@Service("vboService")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class VboServiceImpl extends AuthorizationServiceBase implements VboService {
    private static Logger log = LoggerFactory.getLogger(VboServiceImpl.class);

    @Inject
    private VboServiceImpl _self;

    @Value("${security.vbo.usageevent.url}")
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

        log.info("Sending VBO User Seat Usage Event for user " + usageEvent.getEmailAddress());

        try {
            ResponseEntity<String> response = restTemplate.postForEntity(URI.create(usageEventUrl), usageEvent, String.class);
        } catch (Exception e) {
            log.error("Exception in usage event: " + e.toString());
            throw e;
        }
    }
}
