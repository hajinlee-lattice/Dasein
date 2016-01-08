package com.latticeengines.pls.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.AmAttribute;

@SuppressWarnings("rawtypes")
public interface AmAttributeDao extends BaseDao<AmAttribute> {

    List<AmAttribute> findAttributes(String key, String parentKey, String parentValue);

    AmAttribute findAttributeMeta(String key);

    List<List> findCompanyCount(String key, String parentKey, String parentValue);
    
    List<List> findSubCategoryCount(String key, String parentKey, String parentValue);
}
