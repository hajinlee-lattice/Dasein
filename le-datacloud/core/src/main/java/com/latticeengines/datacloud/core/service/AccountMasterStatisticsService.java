package com.latticeengines.datacloud.core.service;

import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFact;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFactQuery;
import com.latticeengines.domain.exposed.metadata.Category;

import java.util.Map;

public interface AccountMasterStatisticsService {

    AccountMasterFact query(AccountMasterFactQuery query);

    Map<Category, Long> getCategories();

    Map<String, Long> getSubCategories(Category category);

    Long getAttrId (String columnName, String categoricalValue);

}
