package com.latticeengines.datacloud.match.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn;

public interface ExternalColumnDao extends BaseDao<ExternalColumn> {
	List<ExternalColumn> findByTag(String tag);
}
