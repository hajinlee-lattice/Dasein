package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.pls.MarketoMatchField;

import java.util.List;

public interface EnrichmentService {

    void updateEnrichmentMatchFields(String id, List<MarketoMatchField> marketoMatchFields);

}
