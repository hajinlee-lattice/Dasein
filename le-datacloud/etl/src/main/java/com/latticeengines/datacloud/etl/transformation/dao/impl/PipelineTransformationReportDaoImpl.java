package com.latticeengines.datacloud.etl.transformation.dao.impl;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.transformation.dao.PipelineTransformationReportDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.PipelineTransformationReportByStep;

@Component("pipelineTransformationReportDao")
public class PipelineTransformationReportDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<PipelineTransformationReportByStep>
        implements PipelineTransformationReportDao {

    @Override
    protected Class<PipelineTransformationReportByStep> getEntityClass() {
        return PipelineTransformationReportByStep.class;
    }

    @Override
    public void deleteReport(String pipeline, String version) {
        Session session = getSessionFactory().getCurrentSession();
        Class<PipelineTransformationReportByStep> entityClz = getEntityClass();
        String queryStr = String.format(
                "delete from %s where Pipeline = :pipeline and Version = :version",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("pipeline", pipeline);
        query.setString("version", version);
        query.executeUpdate();
    }
}
