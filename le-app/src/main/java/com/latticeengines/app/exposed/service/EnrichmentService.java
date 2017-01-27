package com.latticeengines.app.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFactQuery;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributes;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;

public interface EnrichmentService {

    void updateEnrichmentMatchFields(String id, List<MarketoMatchField> marketoMatchFields);

    AccountMasterCube getCube(AccountMasterFactQuery query);

    AccountMasterCube getCube(String query);

    TopNAttributes getTopAttrs(Category category, int max, boolean shouldConsiderInternalEnrichment);

}
