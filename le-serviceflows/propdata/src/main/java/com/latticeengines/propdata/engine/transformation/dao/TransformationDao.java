package com.latticeengines.propdata.engine.transformation.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.manage.Transformation;

public interface TransformationDao extends BaseDao<Transformation> {

	List<Transformation> findAllForSource(String sourceName);
}
