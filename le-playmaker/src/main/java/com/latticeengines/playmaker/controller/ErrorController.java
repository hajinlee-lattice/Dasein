package com.latticeengines.playmaker.controller;

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
            case 404:
                return get404ModelAndView();
            case 500:
                return get500ModelAndView();
            default:
                //return getUnsupportedModelAndView();
                throw new UnsupportedOperationException("Cannot handle http error " + httpErrorCode);
        }
    }

    private int getErrorCode(HttpServletRequest httpRequest) {
        return (Integer) httpRequest
                .getAttribute("javax.servlet.error.status_code");
    }

    private ModelAndView get404ModelAndView() {
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        return new ModelAndView(jsonView, ImmutableMap.of("errorCode", LedpCode.LEDP_19003.name(), //
                "errorMsg", LedpCode.LEDP_19003.getMessage()));
    }
    
    private ModelAndView get500ModelAndView() {
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        return new ModelAndView(jsonView, ImmutableMap.of("errorCode", LedpCode.LEDP_19013.name(), //
                "errorMsg", LedpCode.LEDP_19013.getMessage()));
    }
    
    private ModelAndView getUnsupportedModelAndView() {
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        return new ModelAndView(jsonView, ImmutableMap.of("errorCode", LedpCode.LEDP_19014.name(), //
                "errorMsg", LedpCode.LEDP_19014.getMessage()));
    }

}