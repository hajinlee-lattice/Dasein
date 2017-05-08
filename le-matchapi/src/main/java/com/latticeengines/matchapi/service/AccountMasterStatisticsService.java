package com.latticeengines.matchapi.service;

import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFactQuery;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributeTree;
import com.latticeengines.domain.exposed.metadata.Category;

public interface AccountMasterStatisticsService {

    AccountMasterCube query(AccountMasterFactQuery query, boolean considerOnlyEnrichments);

    TopNAttributeTree getTopAttrTree();

    Map<Category, Long> getCategories();

    Map<String, Long> getSubCategories(Category category);

    Long getAttrId(String columnName, String categoricalValue);

}
