package com.latticeengines.datacloud.etl.transformation.dao;

import java.util.List;

import com.latticeengines.datacloud.etl.dao.ProgressDao;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;

public interface TransformationProgressDao extends ProgressDao<TransformationProgress> {

    List<TransformationProgress> findAllForBaseSourceVersions(String sourceName, String baseVersions);

}
