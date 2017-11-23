package com.latticeengines.security.exposed.serviceruntime.exception;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.json.MappingJackson2JsonView;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.exception.LedpCode;

@Controller
public class ErrorController {


    @RequestMapping(value = "errors", method = RequestMethod.GET)
    public ModelAndView renderErrorPage(HttpServletRequest httpRequest) {
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

    private ModelAndView get401ModelAndView() {
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        return new ModelAndView(jsonView, ImmutableMap.of("errorCode", LedpCode.LEDP_19001.name(), //
                "errorMsg", LedpCode.LEDP_19001.getMessage()));
    }

    private ModelAndView get403ModelAndView() {
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        return new ModelAndView(jsonView, ImmutableMap.of("errorCode", LedpCode.LEDP_19002.name(), //
                "errorMsg", LedpCode.LEDP_19002.getMessage()));
    }

    private ModelAndView get404ModelAndView() {
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        return new ModelAndView(jsonView, ImmutableMap.of("errorCode", LedpCode.LEDP_19003.name(), //
                "errorMsg", LedpCode.LEDP_19003.getMessage()));
    }
}
