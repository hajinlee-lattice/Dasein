package com.latticeengines.propdata.api.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.MatchCommand;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.network.exposed.propdata.MatchInterface;
import com.latticeengines.propdata.match.service.BulkMatchService;
import com.latticeengines.propdata.match.service.RealTimeMatchService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "match", description = "REST resource for propdata matches")
@RestController
@RequestMapping("/matches")
public class MatchResource implements MatchInterface {

    @Autowired
    private RealTimeMatchService realTimeMatchService;

    @Autowired
    private BulkMatchService bulkMatchService;

    @Value("${camille.zk.pod.id:Default}")
    private String podId;

    @RequestMapping(value = "/realtime", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Match to derived column selection. Specify input fields and MatchKey -> Field mapping. "
            + "Available match keys are Domain, Name, City, State, Country, DUNS, LatticeAccountID. "
            + "Domain can be anything that can be parsed to a domain, such as website, email, etc. "
            + "When domain is not provided, Name, State, Country must be provided. Country is default to USA. "

    )
    public MatchOutput matchRealTime(@RequestBody MatchInput input) {
        try {
            return realTimeMatchService.match(input);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25007, "PropData match failed.",  e);
        }
    }

    @RequestMapping(value = "/bulk", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Match to derived column selection. Same input as realtime match, "
            + "except using InputBuffer instead of embedding Data in json body directly. "
            + "The request parameter podid is used to change the hdfs pod id. "
            + "This parameter is mainly for testing purpose. "
            + "Leave it empty will result in using the pod id defined in camille environment.")
    public MatchCommand matchBulk(@RequestBody MatchInput input,
            @RequestParam(value = "podid", required = false, defaultValue = "") String hdfsPod) {
        try {
            return bulkMatchService.match(input, hdfsPod);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25007, "PropData match failed.",  e);
        }
    }

    @RequestMapping(value = "/bulk/{rootuid}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get match status using rootuid (RootOperationUid).")
    public MatchCommand bulkMatchStatus(@PathVariable String rootuid) {
        try {
            return bulkMatchService.status(rootuid.toUpperCase());
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25008, e, new String[]{ rootuid });
        }
    }

}
