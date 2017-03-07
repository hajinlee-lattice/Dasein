package com.latticeengines.matchapi.controller;

import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.GzipUtils;
import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchInput;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.matchapi.service.BulkMatchService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "match", description = "REST resource for propdata matches")
@RestController
@RequestMapping("/matches")
public class MatchResource {
    private static final Log log = LogFactory.getLog(MatchResource.class);

    private static final ObjectMapper OM = new ObjectMapper();

    @Autowired
    private RealTimeMatchService realTimeMatchService;

    @Autowired
    private List<BulkMatchService> bulkMatchServiceList;

    @Value("${camille.zk.pod.id:Default}")
    private String podId;

    @RequestMapping(value = "/realtime", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Match to derived column selection. Specify input fields and MatchKey -> Field mapping. "
            + "Available match keys are Domain, Name, City, State, Country, DUNS, LatticeAccountID. "
            + "Domain can be anything that can be parsed to a domain, such as website, email, etc. "
            + "When domain is not provided, Name, State, Country must be provided. Country is default to USA. "

    )
    public void matchRealTime(@RequestBody MatchInput input, HttpServletResponse response) {
        try {
            MatchOutput output = realTimeMatchService.match(input);
            GzipUtils.writeToGzipStream(response, output);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25007, "PropData match failed: " + e.getMessage(), e);
        }
    }

    @RequestMapping(value = "/bulkrealtime", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Match to derived column selection. Specify input fields and MatchKey -> Field mapping. "
            + "Available match keys are Domain, Name, City, State, Country, DUNS, LatticeAccountID. "
            + "Domain can be anything that can be parsed to a domain, such as website, email, etc. "
            + "When domain is not provided, Name, State, Country must be provided. Country is default to USA. "

    )
    public void bulkMatchRealTime(@RequestBody BulkMatchInput input, HttpServletResponse response) {
        long time = System.currentTimeMillis();
        try {
            BulkMatchOutput output = realTimeMatchService.matchBulk(input);
            GzipUtils.writeToGzipStream(response, output);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25007, "PropData matchBulk failed.", e);
        } finally {
            log.info((System.currentTimeMillis() - time) + " milli for matching " + input.getInputList().size()
                    + " match inputs");
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
            String matchVersion = input.getDataCloudVersion();
            BulkMatchService bulkMatchService = getBulkMatchService(matchVersion);
            return bulkMatchService.match(input, hdfsPod);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25007, "PropData match failed: " + e.getMessage(), e);
        }
    }

    @RequestMapping(value = "/bulk/{rootuid}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get match status using rootuid (RootOperationUid).")
    public MatchCommand bulkMatchStatus(@PathVariable String rootuid) {
        try {
            BulkMatchService bulkMatchService = getBulkMatchService(null);
            return bulkMatchService.status(rootuid.toUpperCase());
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25008, e, new String[] { rootuid });
        }
    }

    private BulkMatchService getBulkMatchService(String matchVersion) {
        for (BulkMatchService handler : bulkMatchServiceList) {
            if (handler.accept(matchVersion)) {
                return handler;
            }
        }
        throw new LedpException(LedpCode.LEDP_25021, new String[] { matchVersion });
    }

}
