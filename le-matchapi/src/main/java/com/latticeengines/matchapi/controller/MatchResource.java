package com.latticeengines.matchapi.controller;

import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.ImmutableMap;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import com.latticeengines.datacloud.core.annotation.PodContextAware;
import com.latticeengines.datacloud.match.exposed.service.MatchValidationService;
import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchInput;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.BulkMatchWorkflowConfiguration;
import com.latticeengines.matchapi.service.BulkMatchService;


@Api(value = "match", description = "REST resource for propdata matches")
@RestController
@RequestMapping("/matches")
public class MatchResource {
    private static final Logger log = LoggerFactory.getLogger(MatchResource.class);

    @Inject
    private RealTimeMatchService realTimeMatchService;

    @Inject
    private List<BulkMatchService> bulkMatchServiceList;

    @Inject
    private MatchValidationService matchValidationService;

    @Value("${camille.zk.pod.id:Default}")
    private String podId;

    @PostMapping(value = "/realtime")
    @ResponseBody
    @ApiOperation(value = "Match to derived column selection. Specify input fields and MatchKey -> Field mapping. "
            + "Available match keys are Domain, Name, City, State, Country, DUNS, LatticeAccountID. "
            + "Domain can be anything that can be parsed to a domain, such as website, email, etc. "
            + "When domain is not provided, Name, State, Country must be provided. Country is default to USA. "
    )
    public MatchOutput matchRealTime(@RequestBody MatchInput input) {
        matchValidationService.validateDataCloudVersion(input.getDataCloudVersion());

        // Skip logic for setting up mock for CDL lookup if MatchInput has Operational Mode set to LDC Match or
        // Entity Match.
        if (input.getOperationalMode() == null || OperationalMode.CDL_LOOKUP.equals(input.getOperationalMode())) {
            if (MapUtils.isNotEmpty(input.getKeyMap()) && input.getKeyMap().containsKey(MatchKey.LookupId) //
                    && !"AccountId".equals(input.getKeyMap().get(MatchKey.LookupId).get(0))) {
                input = mockForCDLLookup(input);
            }
        }
        return realTimeMatchService.match(input);
    }

    @PostMapping(value = "/bulkrealtime")
    @ResponseBody
    @ApiOperation(value = "Match to derived column selection. Specify input fields and MatchKey -> Field mapping. "
            + "Available match keys are Domain, Name, City, State, Country, DUNS, LatticeAccountID. "
            + "Domain can be anything that can be parsed to a domain, such as website, email, etc. "
            + "When domain is not provided, Name, State, Country must be provided. Country is default to USA. ")
    public BulkMatchOutput bulkMatchRealTime(@RequestBody BulkMatchInput input) {
        long time = System.currentTimeMillis();
        try {
            if (CollectionUtils.isNotEmpty(input.getInputList())) {
                for (MatchInput matchInput : input.getInputList()) {
                    matchValidationService.validateDataCloudVersion(matchInput.getDataCloudVersion());
                }
            }
            return realTimeMatchService.matchBulk(input);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25007, "PropData matchBulk failed.", e);
        } finally {
            log.info((System.currentTimeMillis() - time) + " milli for matching " + input.getInputList().size()
                    + " match inputs");
        }
    }

    @PodContextAware
    @PostMapping(value = "/bulk", produces = "application/json")
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
            matchValidationService.validateDataCloudVersion(matchVersion);
            matchValidationService.validateDecisionGraph(input.getDecisionGraph());
            BulkMatchService bulkMatchService = getBulkMatchService(matchVersion);
            return bulkMatchService.match(input, hdfsPod);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25007, "PropData match failed: " + e.getMessage(), e);
        }
    }

    @PodContextAware
    @PostMapping(value = "/bulkconf", produces = "application/json")
    @ResponseBody
    @ApiOperation(value = "Match to derived column selection. Same input as realtime match, "
            + "except using InputBuffer instead of embedding Data in json body directly. "
            + "The request parameter podid is used to change the hdfs pod id. "
            + "This parameter is mainly for testing purpose. "
            + "Leave it empty will result in using the pod id defined in camille environment.")
    public BulkMatchWorkflowConfiguration getBulkMatchConfig(@RequestBody MatchInput input,
            @RequestParam(value = "podid", required = false, defaultValue = "") String hdfsPod) {
        try {
            String matchVersion = input.getDataCloudVersion();
            matchValidationService.validateDataCloudVersion(matchVersion);
            BulkMatchService bulkMatchService = getBulkMatchService(matchVersion);
            return bulkMatchService.getWorkflowConf(input, hdfsPod);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25007, "PropData match failed: " + e.getMessage(), e);
        }
    }

    @GetMapping(value = "/bulk/{rootuid}", produces = "application/json")
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

    private MatchInput mockForCDLLookup(MatchInput input) {
        input.setCustomSelection(null);
        input.setUnionSelection(null);
        input.setPredefinedSelection(ColumnSelection.Predefined.RTS);
        input.setFetchOnly(true);
        List<String> idFields = new ArrayList<>(input.getKeyMap().get(MatchKey.LookupId));
        input.setKeyMap(ImmutableMap.of(MatchKey.LatticeAccountID, idFields));
        input.setSkipKeyResolution(true);
        return input;
    }

}
