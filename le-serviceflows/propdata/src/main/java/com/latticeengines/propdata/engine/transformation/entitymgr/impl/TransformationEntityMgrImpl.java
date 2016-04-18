package com.latticeengines.propdata.engine.transformation.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.manage.Transformation;
import com.latticeengines.propdata.engine.transformation.dao.TransformationDao;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationEntityMgr;

@Component("transformationEntityMgr")
public class TransformationEntityMgrImpl implements TransformationEntityMgr {

    @Autowired
    private TransformationDao dao;

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public Transformation findByTransformationName(String transformationName) {
        return dao.findByField("TransformationName", transformationName);
    }

    @Override
    @Transactional(value = "propDataManage")
    public Transformation addTransformation(Transformation transformation) {
        dao.create(transformation);
        return dao.findByField("TransformationName", transformation.getTransformationName());
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    public void removeTransformation(String transformationName) {
        Transformation transformation = dao.findByField("TransformationName", transformationName);
        if (transformation != null) {
            dao.delete(transformation);
        }
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<Transformation> findAll() {
        return dao.findAll();
    }

}
