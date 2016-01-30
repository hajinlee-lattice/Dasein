package com.latticeengines.propdata.api.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.MatchInput;
import com.latticeengines.domain.exposed.propdata.manage.MatchOutput;
import com.latticeengines.propdata.match.service.RealTimeMatchService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "match", description = "REST resource for propdata match")
@RestController
@RequestMapping("/match")
public class MatchResource {

    @Autowired
    @Qualifier(value = "realTimeMatchServiceCache")
    private RealTimeMatchService realTimeMatchService;

    @RequestMapping(value = "/realtime", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Match to derived column selection. " +
            "Each input row contains one or many match keys. " +
            "Available match keys are Domain, Name, City, State, Country, DUNS, LatticeAccountID. " +
            "Domain can be anything that can be parsed to a domain, such as website, email, etc. " +
            "When domain is not provided, Name, State, Country must be provided. Country is default to USA."
    )
    public MatchOutput matchSync(@RequestBody MatchInput input) {
        try {
            return realTimeMatchService.match(input, true, true, true);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25007, e);
        }
    }

}
