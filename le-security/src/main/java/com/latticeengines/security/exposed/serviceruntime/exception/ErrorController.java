package com.latticeengines.security.exposed.serviceruntime.exception;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;

@Controller
public class ErrorController {


    @GetMapping(value = "errors")
    @ResponseBody
    public JsonNode renderErrorPage(HttpServletRequest httpRequest) {
        int httpErrorCode = getErrorCode(httpRequest);
        switch (httpErrorCode) {
            case 401:
                return get401ModelAndView();
            case 403:
                return get403ModelAndView();
            case 404:
                return get404ModelAndView();
            default:
                throw new UnsupportedOperationException("Cannot handle http error " + httpErrorCode);
        }
    }

    private int getErrorCode(HttpServletRequest httpRequest) {
        return (Integer) httpRequest
                .getAttribute("javax.servlet.error.status_code");
    }

    private JsonNode get401ModelAndView() {
        return JsonUtils.getObjectMapper().valueToTree(ImmutableMap.of("errorCode", LedpCode.LEDP_19001.name(), //
                "errorMsg", LedpCode.LEDP_19001.getMessage()));
    }

    private JsonNode get403ModelAndView() {
        return JsonUtils.getObjectMapper().valueToTree(ImmutableMap.of("errorCode", LedpCode.LEDP_19002.name(), //
                "errorMsg", LedpCode.LEDP_19002.getMessage()));
    }

    private JsonNode get404ModelAndView() {
        return JsonUtils.getObjectMapper().valueToTree(ImmutableMap.of("errorCode", LedpCode.LEDP_19003.name(), //
                "errorMsg", LedpCode.LEDP_19003.getMessage()));
    }
}
