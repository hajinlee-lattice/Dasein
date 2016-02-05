package com.latticeengines.propdata.api.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.network.exposed.propdata.MatchInterface;
import com.latticeengines.propdata.match.service.RealTimeMatchService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "match", description = "REST resource for propdata matches")
@RestController
@RequestMapping("/matches")
public class MatchResource implements MatchInterface {

    @Autowired
    private RealTimeMatchService realTimeMatchService;

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Match to derived column selection. Specify input fields and MatchKey -> Field mapping. "
            + "Available match keys are Domain, Name, City, State, Country, DUNS, LatticeAccountID. "
            + "Domain can be anything that can be parsed to a domain, such as website, email, etc. "
            + "When domain is not provided, Name, State, Country must be provided. Country is default to USA. "
            + "The url flag \"unmatched\" toggles whether to return the unmatched records."

    )
    public MatchOutput match(@RequestBody MatchInput input,
            @RequestParam(value = "unmatched", required = false, defaultValue = "true") Boolean returnUnmatched) {
        try {
            if (input.getMatchEngine() == null) {
                throw new IllegalArgumentException("Must specify match engine.");
            }
            if (MatchInput.MatchEngine.RealTime.equals(input.getMatchEngine())) {
                return realTimeMatchService.match(input, returnUnmatched);
            } else {
                throw new UnsupportedOperationException(
                        "Match engine " + MatchInput.MatchEngine.Bulk + " is not supported.");
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25007, e);
        }
    }

}
