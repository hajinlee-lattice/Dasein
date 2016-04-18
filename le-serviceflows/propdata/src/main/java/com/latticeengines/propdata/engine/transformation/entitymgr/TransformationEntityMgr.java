package com.latticeengines.propdata.engine.transformation.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.Transformation;

public interface TransformationEntityMgr {

    Transformation findByTransformationName(String transformationName);

    Transformation addTransformation(Transformation transformation);

    void removeTransformation(String transformationName);

    List<Transformation> findAll();
}
