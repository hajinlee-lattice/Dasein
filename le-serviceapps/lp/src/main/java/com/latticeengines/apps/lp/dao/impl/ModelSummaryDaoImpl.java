package com.latticeengines.apps.lp.dao.impl;

import java.util.List;
import java.util.Set;

import javax.persistence.Table;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.hibernate.type.StringType;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.lp.dao.ModelSummaryDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.security.Tenant;

@SuppressWarnings("deprecation")
@Component("modelSummaryDao")
public class ModelSummaryDaoImpl extends BaseDaoImpl<ModelSummary> implements ModelSummaryDao {

    @Override
    protected Class<ModelSummary> getEntityClass() {
        return ModelSummary.class;
    }

    @Override
    public ModelSummary findByApplicationId(String applicationId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where applicationId = :applicationId", entityClz.getSimpleName());
        @SuppressWarnings("unchecked")
        Query<ModelSummary> query = session.createQuery(queryStr);
        query.setString("applicationId", applicationId);
        List<ModelSummary> list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return list.get(0);
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public List<ModelSummary> getModelSummariesByApplicationId(String applicationId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where applicationId = :applicationId", entityClz.getSimpleName());
        Query<ModelSummary> query = session.createQuery(queryStr);
        query.setString("applicationId", applicationId);
        return query.list();
    }

    @Override
    public ModelSummary findByModelId(String modelId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where id = :modelId", entityClz.getSimpleName());
        @SuppressWarnings("unchecked")
        Query<ModelSummary> query = session.createQuery(queryStr);
        query.setString("modelId", modelId);
        List<ModelSummary> list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return list.get(0);
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public List<ModelSummary> getAllByTenant(Tenant tenant) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where tenantId = :tenantId and status != :statusId",
                entityClz.getSimpleName());
        Query<ModelSummary> query = session.createQuery(queryStr);
        query.setLong("tenantId", tenant.getPid());
        query.setInteger("statusId", ModelSummaryStatus.DELETED.getStatusId());
        return query.list();
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public List<String> getAllModelSummaryIds() {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String modelSummaryTable = entityClz.getAnnotation(Table.class).name();
        String sqlStr = String.format("SELECT ModelSummary.ID FROM %s as ModelSummary", modelSummaryTable);
        @SuppressWarnings("rawtypes")
        SQLQuery sqlQuery = session.createSQLQuery(sqlStr).addScalar("ID", new StringType());
        return sqlQuery.list();
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public List<ModelSummary> findAllValid() {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where status != :statusId", entityClz.getSimpleName());
        Query<ModelSummary> query = session.createQuery(queryStr);
        query.setInteger("statusId", ModelSummaryStatus.DELETED.getStatusId());
        return query.list();
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public List<ModelSummary> findAllActive() {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where status = :statusId", entityClz.getSimpleName());
        Query<ModelSummary> query = session.createQuery(queryStr);
        query.setInteger("statusId", ModelSummaryStatus.ACTIVE.getStatusId());
        return query.list();
    }

    @Override
    public int findTotalCount(long lastUpdateTime, boolean considerAllStatus) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String basicQueryStr = "select count(*) from %s where lastUpdateTime >= :lastUpdateTime ";
        if (!considerAllStatus) {
            basicQueryStr += " and status = :statusId ";
        }
        String queryStr = String.format(basicQueryStr, entityClz.getSimpleName());
        @SuppressWarnings("rawtypes")
        Query query = session.createQuery(queryStr);
        query.setLong("lastUpdateTime", lastUpdateTime);
        if (!considerAllStatus) {
            query.setInteger("statusId", ModelSummaryStatus.ACTIVE.getStatusId());
        }
        return ((Long) query.uniqueResult()).intValue();
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public List<ModelSummary> findPaginatedModels(long lastUpdateTime, boolean considerAllStatus, int offset,
            int maximum) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String basicQueryStr = "from %s where lastUpdateTime >= :lastUpdateTime ";
        if (!considerAllStatus) {
            basicQueryStr += " and status = :statusId ";
        }

        basicQueryStr += " order by lastUpdateTime, constructionTime asc";

        String queryStr = String.format(basicQueryStr, entityClz.getSimpleName());
        Query<ModelSummary> query = session.createQuery(queryStr).setFirstResult(offset).setMaxResults(maximum);
        query.setLong("lastUpdateTime", lastUpdateTime);
        if (!considerAllStatus) {
            query.setInteger("statusId", ModelSummaryStatus.ACTIVE.getStatusId());
        }
        return query.list();
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public ModelSummary findValidByModelId(String modelId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where id = :modelId AND status != :statusId",
                entityClz.getSimpleName());
        Query<ModelSummary> query = session.createQuery(queryStr);
        query.setString("modelId", modelId);
        query.setInteger("statusId", ModelSummaryStatus.DELETED.getStatusId());
        List<ModelSummary> list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return list.get(0);
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public ModelSummary getByModelNameInTenant(String modelName, Tenant tenant) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where name = :modelName AND tenantId = :tenantId",
                entityClz.getSimpleName());
        Query<ModelSummary> query = session.createQuery(queryStr);
        query.setString("modelName", modelName);
        query.setLong("tenantId", tenant.getPid());
        List<ModelSummary> list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return list.get(0);
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public List<ModelSummary> getModelSummariesModifiedWithinTimeFrame(long timeFrame) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where :currentTime - lastUpdateTime <= :timeFrame",
                entityClz.getSimpleName());
        Query<ModelSummary> query = session.createQuery(queryStr);
        query.setLong("currentTime", System.currentTimeMillis());
        query.setLong("timeFrame", timeFrame);
        return query.list();
    }

    @SuppressWarnings({ "rawtypes" })
    @Override
    public boolean hasBucketMetadata(String modelId) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("from %s bm where bm.modelSummary.id = :modelId",
                BucketMetadata.class.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("modelId", modelId);
        List list = query.list();
        return CollectionUtils.isNotEmpty(list);
    }

    @Override
    public List<ModelSummary> findModelSummariesByIds(Set<String> ids) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where id in (:ids)",
                entityClz.getSimpleName());
        Query<ModelSummary> query = session.createQuery(queryStr);
        query.setParameter("ids", ids);
        return query.list();
    }

}
