package com.latticeengines.modelquality.dao.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.AnalyticTest;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.modelquality.dao.ModelRunDao;

@Component("qualityModelRunDao")
public class ModelRunDaoImpl extends ModelQualityBaseDaoImpl<ModelRun> implements ModelRunDao {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ModelRunDaoImpl.class);

    @Override
    protected Class<ModelRun> getEntityClass() {
        return ModelRun.class;
    }

    @Override
    public List<ModelRun> findAllByAnalyticTest(AnalyticTest analyticTest) {
        Session session = getSessionFactory().getCurrentSession();

        String queryStr = String.format("from %s where ANALYTIC_TEST_NAME = :atName " + //
                "AND FK_ANALYTIC_PIPELINE_ID IN (:ids)", getEntityClass().getSimpleName());
        Query<ModelRun> query = session.createQuery(queryStr, ModelRun.class);
        query.setParameter("atName", analyticTest.getName());
        query.setParameterList("ids",
                analyticTest.getAnalyticPipelines() //
                        .stream().map(AnalyticPipeline::getPid) //
                        .collect(Collectors.toList()));
        return (List<ModelRun>) query.list();
    }

}
