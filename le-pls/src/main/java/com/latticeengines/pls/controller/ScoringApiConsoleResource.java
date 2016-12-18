package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.collections.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.scoringapi.DebugScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.network.exposed.scoringapi.InternalScoringApiInterface;
import com.latticeengines.pls.service.SelectedAttrService;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.util.SecurityUtils;
import com.wordnik.swagger.annotations.ApiParam;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "scores/apiconsole", description = "REST resource for interacting with scoringapi via APIConsole")
@RestController
@RequestMapping("/scores/apiconsole")
@PreAuthorize("hasRole('View_PLS_Data')")
public class ScoringApiConsoleResource {

    @Autowired
    protected InternalScoringApiInterface internalScoringApiProxy;

    @Autowired
    private SessionService sessionService;

    @Autowired
    private SelectedAttrService selectedAttrService;

    @RequestMapping(value = "/record/debug", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Score a record including debug info such as probability via APIConsole")
    public DebugScoreResponse scoreAndEnrichRecordApiConsole(HttpServletRequest request, //
            @ApiParam(value = "Should load enrichment attribute metadata", //
                    required = false) //
            @RequestParam(value = "shouldSkipLoadingEnrichmentMetadata", required = false, defaultValue = "false") //
            Boolean shouldSkipLoadingEnrichmentMetadata, //
            @ApiParam(value = "Should enforce fuzzy match", //
                    required = false) //
            @RequestParam(value = "enforceFuzzyMatch", required = false, defaultValue = "true") //
            Boolean enforceFuzzyMatch, //
            @RequestBody ScoreRequest scoreRequest) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        boolean enrichmentEnabledForInternalAttributes = FeatureFlagClient.isEnabled(
                CustomerSpace.parse(tenant.getId()),
                LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES.getName());

        if (enforceFuzzyMatch == null) {
            enforceFuzzyMatch = true;
        }
        DebugScoreResponse resp = internalScoringApiProxy.scoreAndEnrichRecordApiConsole(scoreRequest, tenant.getId(),
                enrichmentEnabledForInternalAttributes, enforceFuzzyMatch);

        Map<String, Object> enrichValueMap = resp.getEnrichmentAttributeValues();
        Map<String, Object> nonNullEnrichValueMap = new HashMap<>();

        resp.setEnrichmentAttributeValues(nonNullEnrichValueMap);
        resp.setMatchedRecord(null);
        resp.setMatchedRecordTypes(null);
        resp.setTransformedRecord(null);
        resp.setTransformedRecordTypes(null);

        if (!MapUtils.isEmpty(enrichValueMap)) {
            boolean needEnrichmentMetadataLoading = false;

            for (String enrichKey : enrichValueMap.keySet()) {
                Object value = enrichValueMap.get(enrichKey);
                if (value != null) {
                    nonNullEnrichValueMap.put(enrichKey, value);
                    needEnrichmentMetadataLoading = true;
                }
            }

            needEnrichmentMetadataLoading = //
                    needEnrichmentMetadataLoading && !shouldSkipLoadingEnrichmentMetadata;

            if (needEnrichmentMetadataLoading) {
                List<LeadEnrichmentAttribute> fullEnrichmentMetadataList = selectedAttrService.getAttributes(tenant,
                        null, null, null, null, null, null, null);

                List<LeadEnrichmentAttribute> requiredEnrichmentMetadataList = new ArrayList<>();

                for (LeadEnrichmentAttribute attr : fullEnrichmentMetadataList) {
                    if (nonNullEnrichValueMap.containsKey(attr.getFieldName())) {
                        requiredEnrichmentMetadataList.add(attr);
                    }
                }

                resp.setEnrichmentMetadataList(requiredEnrichmentMetadataList);
            }
        }

        return resp;
    }
}
