package com.latticeengines.pls.dao.impl;

import java.util.Arrays;
import java.util.List;

import javax.persistence.Table;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.hibernate.type.StringType;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.ModelSummaryDao;

@Component("modelSummaryDao")
public class ModelSummaryDaoImpl extends BaseDaoImpl<ModelSummary> implements ModelSummaryDao {

    @Override
    protected Class<ModelSummary> getEntityClass() {
        return ModelSummary.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ModelSummary findByApplicationId(String applicationId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where applicationId = :applicationId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("applicationId", applicationId);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (ModelSummary) list.get(0);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ModelSummary> getModelSummariesByApplicationId(String applicationId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where applicationId = :applicationId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("applicationId", applicationId);
        return query.list();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ModelSummary findByModelId(String modelId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where id = :modelId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("modelId", modelId);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (ModelSummary) list.get(0);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ModelSummary findByModelName(String modelName) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where name = :modelName", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("modelName", modelName);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (ModelSummary) list.get(0);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ModelSummary> getAllByTenant(Tenant tenant) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where tenantId = :tenantId and status != :statusId",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("tenantId", tenant.getPid());
        query.setInteger("statusId", ModelSummaryStatus.DELETED.getStatusId());
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<String> getAllModelSummaryIds() {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String modelSummaryTable = entityClz.getAnnotation(Table.class).name();
        String sqlStr = String.format("SELECT ModelSummary.ID FROM %s as ModelSummary", modelSummaryTable);
        SQLQuery sqlQuery = session.createSQLQuery(sqlStr).addScalar("ID", new StringType());
        return sqlQuery.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ModelSummary> findAllValid() {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where status != :statusId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setInteger("statusId", ModelSummaryStatus.DELETED.getStatusId());
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ModelSummary> findAllActive() {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where status = :statusId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setInteger("statusId", ModelSummaryStatus.ACTIVE.getStatusId());
        return query.list();
    }

    @Override
    public int findTotalCount(long lastUpdateTime, boolean considerAllStatus, boolean considerDeleted) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String basicQueryStr = "select count(*) from %s where lastUpdateTime >= :lastUpdateTime ";
        if (considerAllStatus) {
            if (!considerDeleted) {
                basicQueryStr += " and status IN (:statusIds) ";
            }
        } else {
            basicQueryStr += " and status = :statusId ";
        }
        String queryStr = String.format(basicQueryStr, entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("lastUpdateTime", lastUpdateTime);
        if (considerAllStatus) {
            if (!considerDeleted) {
                List<ModelSummaryStatus> statusIds = Arrays.asList(ModelSummaryStatus.ACTIVE,
                        ModelSummaryStatus.INACTIVE);
                query.setParameterList("statusIds", statusIds);
            }
        } else {
            query.setInteger("statusId", ModelSummaryStatus.ACTIVE.getStatusId());
        }
        return ((Long) query.uniqueResult()).intValue();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ModelSummary> findPaginatedModels(long lastUpdateTime, boolean considerAllStatus, int offset,
            int maximum, boolean considerDeleted) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String basicQueryStr = "from %s where lastUpdateTime >= :lastUpdateTime ";
        if (considerAllStatus) {
            if (!considerDeleted) {
                basicQueryStr += " and status IN (:statusIds) ";
            }
        } else {
            basicQueryStr += " and status = :statusId ";
        }

        basicQueryStr += " order by lastUpdateTime, constructionTime asc";

        String queryStr = String.format(basicQueryStr, entityClz.getSimpleName());
        Query query = session.createQuery(queryStr).setFirstResult(offset).setMaxResults(maximum);
        query.setLong("lastUpdateTime", lastUpdateTime);
        if (considerAllStatus) {
            if (!considerDeleted) {
                List<ModelSummaryStatus> statusIds = Arrays.asList(ModelSummaryStatus.ACTIVE,
                        ModelSummaryStatus.INACTIVE);
                query.setParameterList("statusIds", statusIds);
            }
        } else {
            query.setInteger("statusId", ModelSummaryStatus.ACTIVE.getStatusId());
        }
        return query.list();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ModelSummary findValidByModelId(String modelId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where id = :modelId AND status != :statusId",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("modelId", modelId);
        query.setInteger("statusId", ModelSummaryStatus.DELETED.getStatusId());
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (ModelSummary) list.get(0);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ModelSummary getByModelNameInTenant(String modelName, Tenant tenant) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where name = :modelName AND tenantId = :tenantId",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("modelName", modelName);
        query.setLong("tenantId", tenant.getPid());
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (ModelSummary) list.get(0);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public List<ModelSummary> getModelSummariesModifiedWithinTimeFrame(long timeFrame) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where :currentTime - lastUpdateTime <= :timeFrame",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("currentTime", System.currentTimeMillis());
        query.setLong("timeFrame", timeFrame);
        return query.list();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
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
}
