package com.latticeengines.scoringapi.exposed.exception;

import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.InsufficientAuthenticationException;
import org.springframework.security.oauth2.common.exceptions.OAuth2Exception;
import org.springframework.security.oauth2.provider.error.DefaultWebResponseExceptionTranslator;
import org.springframework.web.util.HtmlUtils;

public class ExceptionEncodingTranslator extends DefaultWebResponseExceptionTranslator {

    @Override
    public ResponseEntity<OAuth2Exception> translate(Exception e) throws Exception {
        Exception ex = new InsufficientAuthenticationException(HtmlUtils.htmlEscape(e.getMessage()));
        return super.translate(ex);
    }
}