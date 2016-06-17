package com.latticeengines.propdata.match.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;

public interface ExternalColumnDao extends BaseDao<ExternalColumn> {
	List<ExternalColumn> findByTag(String tag);
}
