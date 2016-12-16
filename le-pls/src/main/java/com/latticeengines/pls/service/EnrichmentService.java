package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFactQuery;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributeTree;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributes;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;

import java.util.List;

public interface EnrichmentService {

    void updateEnrichmentMatchFields(String id, List<MarketoMatchField> marketoMatchFields);

    AccountMasterCube getCube(AccountMasterFactQuery query);

    AccountMasterCube getCube(String query);

    TopNAttributes getTopAttrs(Category category);

}
