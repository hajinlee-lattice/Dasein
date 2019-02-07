package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResponseErrorHandler;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class GetDnBResponseErrorHandler implements ResponseErrorHandler {
    private static final Logger log = LoggerFactory.getLogger(GetDnBResponseErrorHandler.class);

    public GetDnBResponseErrorHandler() {

    }

    @Override
    public boolean hasError(ClientHttpResponse response) throws IOException {
        return response.getStatusCode() != HttpStatus.OK;
    }

    @Override
    public void handleError(ClientHttpResponse response) throws IOException {
        log.info(String.format("Response body with HTTPStatus %s: %s", response.getStatusCode().name(),
                IOUtils.toString(response.getBody(), "UTF-8")));
        switch (response.getStatusCode()) {
        case UNAUTHORIZED:
        case REQUEST_TIMEOUT:
        case FORBIDDEN:
            throw new HttpClientErrorException(response.getStatusCode());
        case BAD_REQUEST:
            throw new LedpException(LedpCode.LEDP_25037);
        case NOT_FOUND:
            throw new LedpException(LedpCode.LEDP_25038);
        case INTERNAL_SERVER_ERROR:
        case SERVICE_UNAVAILABLE:
            throw new LedpException(LedpCode.LEDP_25039);
        default:
            throw new LedpException(LedpCode.LEDP_25040,
                    new String[] { String.valueOf(response.getStatusCode().value()), response.getStatusCode().name() });
        }

    }
}
