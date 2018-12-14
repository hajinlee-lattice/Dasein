package com.latticeengines.objectapi.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.domain.exposed.monitor.annotation.NoMetricsLog;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "health", description = "REST resource for checking health of Object API")
@RestController
@RequestMapping("/health")
public class HealthResource {

//    private static final Logger log = LoggerFactory.getLogger(HealthResource.class);
//
//    @Inject
//    private LogTestResource logTestResource;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Health check")
    @NoMetricsLog
    public StatusDocument healthCheck() {
        return StatusDocument.online();
    }

//    @GetMapping("/start")
//    @ResponseBody
//    public String startTest() {
//        log.info("Start logging test.");
//        LogTestRequest request = new LogTestRequest();
//        request.source = "objectapi";
//        request.gaToken = loginPls();
//        logTestResource.pingPls("LocalTest", request);
//        return "ok";
//    }
//
//    private String loginPls() {
//        RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
//        Credentials creds = new Credentials();
//        creds.setUsername("pls-super-admin-tester@test.lattice-engines.com");
//        creds.setPassword("8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918");
//        LoginDocument loginDocument = restTemplate //
//                .postForObject("https://localhost:9081/pls/login", creds, LoginDocument.class);
//        String token = loginDocument.getData();
//        log.info("Get a GA token: " + token);
//        List<ClientHttpRequestInterceptor> interceptors = restTemplate.getInterceptors();
//        interceptors.add(new AuthorizationHeaderHttpRequestInterceptor(token));
//        restTemplate.setInterceptors(interceptors);
//        Tenant tenant = new Tenant();
//        tenant.setId("LocalTest.LocalTest.Production");
//        tenant.setName("LocalTest");
//        restTemplate.postForObject("https://localhost:9081/pls/attach", tenant, String.class);
//        log.info("Attached to LocalTest.");
//        return token;
//    }

}
