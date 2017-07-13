package com.latticeengines.liaison.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.liaison.exposed.service.LiaisonResponse;

@RestController
public class LiaisonResource {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(LiaisonResource.class);

    @RequestMapping(value = "/testresponse", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    public LiaisonResponse liaisonResponse() {

        return new LiaisonResponse(LiaisonResponse.Report.SUCCESS);

    }

}
