package com.latticeengines.scoringapi.controller;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.rest.RequestIdUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.EnrichRequest;
import com.latticeengines.domain.exposed.scoringapi.EnrichResponse;
import com.latticeengines.scoringapi.exposed.ScoreUtils;
import com.latticeengines.scoringinternalapi.controller.BaseEnrich;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "enrich", description = "REST resource for retrieving enrichment values")
@RestController
@RequestMapping(value = "/enrich")
public class EnrichResource extends BaseEnrich {

    @Autowired
    private BatonService batonService;

    @RequestMapping(value = "/record/{uuid}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get enrichment values for one record")
    public EnrichResponse getLeadEnrichmentValues(HttpServletRequest request, @PathVariable String uuid,
            @RequestBody EnrichRequest enrichRequest) {
        // TODO: M26 change to lookup Enrichment entity table
        Pair<String, String> unpacked = UuidUtils.unpackPairUuid(uuid);
        String tenantId = unpacked.getKey();
        String credentialId = unpacked.getValue();
        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);

        EnrichResponse enrichResponse = enrichRecord(request, enrichRequest, customerSpace, //
                ScoreUtils.canEnrichInternalAttributes(batonService, customerSpace), credentialId);

        String requestId = RequestIdUtils.getRequestIdentifierId(request);
        enrichResponse.setRequestId(requestId);
        return enrichResponse;
    }
}
