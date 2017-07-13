package com.latticeengines.scoringapi.enrich.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.stereotype.Component;

import com.google.common.base.Strings;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.scoringapi.EnrichRequest;
import com.latticeengines.domain.exposed.scoringapi.EnrichResponse;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.scoringapi.enrich.EnrichRequestProcessor;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.exposed.exception.ScoringApiException;
import com.latticeengines.scoringapi.match.Matcher;
import com.latticeengines.scoringapi.score.impl.BaseRequestProcessorImpl;

@Component("enrichRequestProcessor")
public class EnrichRequestProcessorImpl extends BaseRequestProcessorImpl implements EnrichRequestProcessor {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(EnrichRequestProcessorImpl.class);

    @Override
    public EnrichResponse process(CustomerSpace space, EnrichRequest request, String requestId) {
        if (StringUtils.isBlank(request.getDomain())
                && StringUtils.isBlank(request.getCompany())
                && StringUtils.isBlank(request.getDUNS())) {
            throw new ScoringApiException(LedpCode.LEDP_31199);
        }
        requestInfo.put("Source", Strings.nullToEmpty(request.getSource()));

        Map<String, FieldSchema> fieldSchemas = new HashMap<>();
        fieldSchemas.put("domain", new FieldSchema(FieldSource.REQUEST, FieldType.STRING, FieldInterpretation.Domain));
        fieldSchemas.put("duns", new FieldSchema(FieldSource.REQUEST, FieldType.STRING, FieldInterpretation.DUNS));
        fieldSchemas.put("companyName",
                new FieldSchema(FieldSource.REQUEST, FieldType.STRING, FieldInterpretation.CompanyName));
        fieldSchemas.put("companyState",
                new FieldSchema(FieldSource.REQUEST, FieldType.STRING, FieldInterpretation.State));
        fieldSchemas.put("companyCountry",
                new FieldSchema(FieldSource.REQUEST, FieldType.STRING, FieldInterpretation.Country));
        Map<String, Object> record = new HashMap<>();
        record.put("domain", request.getDomain());
        record.put("companyName", request.getCompany());
        record.put("companyState", request.getState());
        record.put("companyCountry", request.getCountry());
        record.put("duns", request.getDUNS());
        InterpretedFields interpreted = new InterpretedFields();
        interpreted.setDomain("domain");
        interpreted.setCompanyName("companyName");
        interpreted.setCompanyState("companyState");
        interpreted.setCompanyCountry("companyCountry");
        split("requestPreparation");

        Map<String, Object> enrichmentAttributes = null;

        Map<String, Map<String, Object>> matchedRecordEnrichmentMap = //
                getMatcher(false).matchAndJoin(space, interpreted, //
                        fieldSchemas, record, null, true, false, false, requestId, false, new ArrayList<String>(),
                        new ArrayList<String>(), false);
        enrichmentAttributes = extractMap(matchedRecordEnrichmentMap, Matcher.ENRICHMENT);
        if (enrichmentAttributes == null) {
            enrichmentAttributes = new HashMap<>();
        }
        split("matchRecord");

        EnrichResponse enrichResponse = new EnrichResponse();
        enrichResponse.setEnrichmentAttributeValues(enrichmentAttributes);
        enrichResponse.setTimestamp(timestampFormatter.print(DateTime.now(DateTimeZone.UTC)));

        return enrichResponse;
    }
}
