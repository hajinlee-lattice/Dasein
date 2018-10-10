package com.latticeengines.datacloud.etl.transformation.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.etl.transformation.dao.TransformationProgressDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;

@Component("transformationProgressDao")
public class TransformationProgressDaoImpl
        extends BaseDaoWithAssignedSessionFactoryImpl<TransformationProgress>
        implements TransformationProgressDao {

    @Override
    protected Class<TransformationProgress> getEntityClass() {
        return TransformationProgress.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TransformationProgress findByRootOperationUid(String uid) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where RootOperationUID = :rootUid",
                getEntityClass().getSimpleName());
        Query<TransformationProgress> query = session.createQuery(queryStr);
        query.setParameter("rootUid", uid.toUpperCase());
        List<TransformationProgress> list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return list.get(0);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TransformationProgress> findFailedProgresses(Source source) {
        String sourceName = source.getSourceName();
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format(
                "from %s where SourceName = :sourceName and Status = 'FAILED' order by LatestStatusUpdate asc",
                getEntityClass().getSimpleName());
        Query<TransformationProgress> query = session.createQuery(queryStr);
        query.setParameter("sourceName", sourceName);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TransformationProgress> findUnfinishedProgresses(Source source) {
        String sourceName = source.getSourceName();
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where SourceName = :sourceName "
                + "and Status != 'FINISHED' and Status != 'FAILED' " + "order by CreateTime asc",
                getEntityClass().getSimpleName());
        Query<TransformationProgress> query = session.createQuery(queryStr);
        query.setParameter("sourceName", sourceName);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TransformationProgress> findAllOfSource(Source source) {
        String sourceName = source.getSourceName();
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where SourceName = :sourceName",
                getEntityClass().getSimpleName());
        Query<TransformationProgress> query = session.createQuery(queryStr);
        query.setParameter("sourceName", sourceName);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TransformationProgress> findAllForBaseSourceVersions(String sourceName,
            String baseVersions) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format(
                "from %s where SourceName = :sourceName and BaseSourceVersions = :baseVersions",
                getEntityClass().getSimpleName());
        Query<TransformationProgress> query = session.createQuery(queryStr);
        query.setParameter("sourceName", sourceName);
        query.setParameter("baseVersions", baseVersions);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public TransformationProgress findPipelineAtVersion(String pipelineName, String version) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format(
                "from %s where PipelineName = :pipelineName and Version = :version order by ProgressID desc",
                getEntityClass().getSimpleName());
        Query<TransformationProgress> query = session.createQuery(queryStr);
        query.setParameter("pipelineName", pipelineName);
        query.setParameter("version", version);
        List<TransformationProgress> list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return list.get(0);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TransformationProgress> findFailedPipelines(String pipelineName) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format(
                "from %s where PipelineName = :pipelineName and Status = 'FAILED' order by LatestStatusUpdate asc",
                getEntityClass().getSimpleName());
        Query<TransformationProgress> query = session.createQuery(queryStr);
        query.setParameter("pipelineName", pipelineName);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TransformationProgress> findUnfinishedPipelines(String pipelineName) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where PipelineName = :pipelineName "
                + "and Status != 'FINISHED' and Status != 'FAILED' " + "order by CreateTime asc",
                getEntityClass().getSimpleName());
        Query<TransformationProgress> query = session.createQuery(queryStr);
        query.setParameter("pipelineName", pipelineName);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TransformationProgress> findAllforPipeline(String pipelineName) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where PipelineName = :pipelineName",
                getEntityClass().getSimpleName());
        Query<TransformationProgress> query = session.createQuery(queryStr);
        query.setParameter("pipelineName", pipelineName);
        return query.list();
    }

}
