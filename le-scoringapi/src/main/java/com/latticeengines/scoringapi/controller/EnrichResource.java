package com.latticeengines.scoringapi.controller;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.MDC;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.rest.RequestIdUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.EnrichRequest;
import com.latticeengines.domain.exposed.scoringapi.EnrichResponse;
import com.latticeengines.monitor.exposed.annotation.InvocationMeter;
import com.latticeengines.monitor.exposed.metrics.impl.InstrumentRegistry;
import com.latticeengines.scoringapi.exposed.ScoreUtils;
import com.latticeengines.scoringinternalapi.controller.BaseEnrich;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "enrich", description = "REST resource for retrieving enrichment values")
@RestController
@RequestMapping("/enrich")
public class EnrichResource extends BaseEnrich {

    private static final String ENRICH_RECORD_INSTRUMENT = "EnrichRecord";

    @Inject
    private BatonService batonService;

    @PostConstruct
    public void postConstruct() {
        InstrumentRegistry.register(ENRICH_RECORD_INSTRUMENT, new EnrichRecordInstrument());
    }

    @PostMapping("/record/{uuid}")
    @ResponseBody
    @ApiOperation(value = "Get enrichment values for one record")
    @InvocationMeter(name = "enrich", measurment = "scoringapi", instrument = ENRICH_RECORD_INSTRUMENT)
    public EnrichResponse getLeadEnrichmentValues(HttpServletRequest request, @PathVariable String uuid,
            @RequestBody EnrichRequest enrichRequest) {
        // TODO: M26 change to lookup Enrichment entity table
        Pair<String, String> unpacked = UuidUtils.unpackPairUuid(uuid);
        String tenantId = unpacked.getKey();
        String credentialId = unpacked.getValue();
        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
        setCustomerSpaceInMDC(customerSpace);
        try {
            EnrichResponse enrichResponse = enrichRecord(request, enrichRequest, customerSpace, //
                    ScoreUtils.canEnrichInternalAttributes(batonService, customerSpace), credentialId);

            String requestId = RequestIdUtils.getRequestIdentifierId(request);
            enrichResponse.setRequestId(requestId);
            return enrichResponse;
        } finally {
            removeCustomerSpaceFromMDC();
        }
    }

    private void setCustomerSpaceInMDC(CustomerSpace customerSpace) {
        MDC.put("customerspace", CustomerSpace.shortenCustomerSpace(customerSpace.toString()));
    }

    private void removeCustomerSpaceFromMDC() {
        MDC.remove("customerspace");
    }
}
