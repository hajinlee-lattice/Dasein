package com.latticeengines.liaison.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.liaison.exposed.service.LiaisonResponse;

@RestController
public class LiaisonResource {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(LiaisonResource.class);

    @RequestMapping(value = "/testresponse", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    public LiaisonResponse liaisonResponse() {

        return new LiaisonResponse(LiaisonResponse.Report.SUCCESS);

    }

}
