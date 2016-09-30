package com.latticeengines.propdata.engine.transformation.dao;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.propdata.collection.dao.ProgressDao;

public interface TransformationProgressDao extends ProgressDao<TransformationProgress> {

    List<TransformationProgress> findAllForBaseSourceVersions(String sourceName, String baseVersions);

}
