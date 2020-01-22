package com.latticeengines.pls.controller;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.AttributeService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.scoringapi.DebugScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.Fields;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.scoringapi.ModelDetail;
import com.latticeengines.domain.exposed.scoringapi.ModelType;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.network.exposed.scoringapi.InternalScoringApiInterface;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.util.SecurityUtils;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "scoringapi-internal", description = "REST resource for interacting with internal scoringapi")
@RestController
@RequestMapping("scoringapi-internal")
@PreAuthorize("hasRole('View_PLS_Data')")
public class ScoringApiInternalResource {

    @Inject
    protected InternalScoringApiInterface internalScoringApiProxy;

    @Inject
    private SessionService sessionService;

    @Inject
    private AttributeService attributeService;

    @Inject
    private BatonService batonService;

    @RequestMapping(value = "/record/apiconsole/debug", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Score a record including debug info such as probability via APIConsole")
    public DebugScoreResponse scoreAndEnrichRecordApiConsole(HttpServletRequest request, //
            @ApiParam(value = "Should load enrichment attribute metadata", //
            required = false)//
            @RequestParam(value = "shouldSkipLoadingEnrichmentMetadata", required = false, defaultValue = "false")//
            Boolean shouldSkipLoadingEnrichmentMetadata, //
            @ApiParam(value = "Should enforce fuzzy match", //
            required = false)//
            @RequestParam(value = "enforceFuzzyMatch", required = false, defaultValue = "true")//
            Boolean enforceFuzzyMatch, //
            @ApiParam(value = "For fuzzy match, should skip DnB cache", //
            required = false)//
            @RequestParam(value = "skipDnBCache", required = false, defaultValue = "false")//
            Boolean skipDnBCache, //
            @RequestBody ScoreRequest scoreRequest) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        boolean enrichmentEnabledForInternalAttributes = batonService
                .isEnabled(CustomerSpace.parse(tenant.getId()), LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES);

        if (enforceFuzzyMatch == null) {
            enforceFuzzyMatch = true;
        }
        if (skipDnBCache == null) {
            skipDnBCache = false;
        }
        DebugScoreResponse resp = internalScoringApiProxy.scoreAndEnrichRecordApiConsole(scoreRequest, tenant.getId(),
                true, enforceFuzzyMatch, skipDnBCache);

        Map<String, Object> enrichValueMap = resp.getEnrichmentAttributeValues();
        Map<String, Object> nonNullEnrichValueMap = new HashMap<>();
        Map<String, Object> nonNullInternalEnrichValueMap = new HashMap<>();

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

            List<LeadEnrichmentAttribute> fullEnrichmentMetadataList = attributeService.getAttributes(tenant, null,
                    null, null, null, null, null, true);

            List<LeadEnrichmentAttribute> requiredEnrichmentMetadataList = new ArrayList<>();

            for (LeadEnrichmentAttribute attr : fullEnrichmentMetadataList) {
                if (nonNullEnrichValueMap.containsKey(attr.getFieldName())) {
                    requiredEnrichmentMetadataList.add(attr);
                    if (attr.getIsInternal()) {
                        nonNullInternalEnrichValueMap.put(attr.getFieldName(),
                                nonNullEnrichValueMap.get(attr.getFieldName()));
                    }
                }
            }
            resp.setCompanyInfo(nonNullInternalEnrichValueMap);

            if (!enrichmentEnabledForInternalAttributes) {
                for (String key : nonNullInternalEnrichValueMap.keySet()) {
                    nonNullEnrichValueMap.remove(key);
                }
            }

            if (needEnrichmentMetadataLoading) {
                resp.setEnrichmentMetadataList(requiredEnrichmentMetadataList);
            }
        }

        return resp;
    }
    
    @RequestMapping(value = "/modeldetails", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get paginated list of models for specified criteria")
    public List<ModelDetail> getPaginatedModels(HttpServletRequest request,
            @ApiParam(value = "The UTC timestamp of last modification in ISO8601 format", required = false) @RequestParam(value = "start", required = false) String start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum,
            @ApiParam(value = "Should consider models in any status or only in active status", required = true) @RequestParam(value = "considerAllStatus", required = true) boolean considerAllStatus,
            @ApiParam(value = "Should consider deleted models as well", required = false) @RequestParam(value = "considerDeleted", required = false, defaultValue = "false") boolean considerDeleted) throws ParseException {

        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);

        Date startDate = null;
        if (!StringUtils.isEmpty(start) && start.contains("%2B")) {
            start = start.replace("%2B", "+");
            startDate = DateTimeUtils.convertToDateUTCISO8601(start);
        }
        return internalScoringApiProxy.getPaginatedModels(startDate, considerAllStatus, offset, maximum, tenant.getId(), considerDeleted);
    }

    @RequestMapping(value = "/models", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Active Models")
    public List<Model> getActiveModels(HttpServletRequest request, @RequestParam(value = "type", required = false) ModelType type) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        return internalScoringApiProxy.getActiveModels(type, tenant.getId());
    }
    
    @RequestMapping(value = "/models/{modelId}/fields", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get fields for a model")
    public Fields getModelFields(HttpServletRequest request, @PathVariable String modelId) {

        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);

        return internalScoringApiProxy.getModelFields(modelId, tenant.getId());
    }
}
