package com.latticeengines.microservice.controller;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.util.SSLUtils;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "status", description = "REST resource for checking status of all microservices")
@RestController
@RequestMapping("/status")
public class StatusController {

    @Value("${microservice.rest.endpoint.hostport}")
    protected String microserviceHostPort;

    @Value("${microservices}")
    protected String microservicesStr;

    private RestTemplate restTemplate = new RestTemplate();

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "check that all the microservices are up")
    public Map<String, String> statusCheck() {
        try {
            SSLUtils.turnOffSslChecking();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        String [] microservices = microservicesStr.split(",");
        Map<String, String> status = new HashMap<>();
        Boolean overall = true;
        for (String microservice : microservices) {
            try {
                String response = restTemplate.getForObject(String.format("%s/%s/api-docs", microserviceHostPort, microservice), String.class);
                if (response.contains("\"apiVersion\"")) {
                    status.put(microservice, "OK");
                } else {
                    status.put(microservice, "Unknow api-doc: " + response);
                    overall = false;
                }
            } catch (Exception e) {
                status.put(microservice, ExceptionUtils.getFullStackTrace(e));
                overall = false;
            }
        }

        status.put("Overall", overall ? "OK" : "ERROR");

        return status;
    }
}
