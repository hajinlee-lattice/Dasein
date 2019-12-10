package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.nio.charset.Charset;

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

    /*
     * (non-Javadoc)
     *
     * @see org.springframework.web.client.ResponseErrorHandler#handleError(org.
     * springframework.http.client.ClientHttpResponse)
     */
    @Override
    public void handleError(ClientHttpResponse response) throws IOException {
        String responseBody = IOUtils.toString(response.getBody(), "UTF-8");
        log.info(String.format("Response body with HTTPStatus %s: %s", response.getStatusCode().name(), responseBody));
        switch (response.getStatusCode()) {
        case UNAUTHORIZED:
        case REQUEST_TIMEOUT:
        case FORBIDDEN:
            throw new HttpClientErrorException(response.getStatusCode(), response.getStatusCode().name(),
                    responseBody.getBytes(), Charset.forName("UTF-8"));
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
