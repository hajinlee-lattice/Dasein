package com.latticeengines.datacloud.etl.ingestion.dao.impl;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.CronUtils;
import com.latticeengines.datacloud.core.util.PropDataConstants;
import com.latticeengines.datacloud.etl.ingestion.dao.IngestionProgressDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;

@Component("ingestionProgressDao")
public class IngestionProgressDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<IngestionProgress> implements
        IngestionProgressDao {
    private static final Logger log = LoggerFactory.getLogger(IngestionProgressDaoImpl.class);

    @Override
    protected Class<IngestionProgress> getEntityClass() {
        return IngestionProgress.class;
    }

    /* 
     * Map Fields: ColumnName -> Value
     * Requires to pass in column name in table, not variable name in IngestionProgress entity class
     */
    @SuppressWarnings("unchecked")
    @Override
    public List<IngestionProgress> getProgressesByField(Map<String, Object> fields, List<String> orderFields) {
        Session session = getSessionFactory().getCurrentSession();
        Class<IngestionProgress> entityClz = getEntityClass();
        StringBuilder sb = new StringBuilder();
        for (String column : fields.keySet()) {
            sb.append(column + " = :" + column + " and ");
        }
        String orderStr = "";
        if (CollectionUtils.isNotEmpty(orderFields)) {
            orderStr = "order by " + String.join(", ", orderFields);
        }
        String queryStr = String.format("from %s where %s %s", entityClz.getSimpleName(),
                sb.substring(0, sb.length() - 4), orderStr);
        Query query = session.createQuery(queryStr);
        for (String column : fields.keySet()) {
            if (fields.get(column).getClass().isEnum()) {
                query.setParameter(column, fields.get(column).toString());
            } else {
                query.setParameter(column, fields.get(column));
            }
        }
        return query.list();
    }

    @Override
    public IngestionProgress saveProgress(IngestionProgress progress) {
        Session session = getSessionFactory().getCurrentSession();
        session.saveOrUpdate(progress);
        return progress;
    }

    @Override
    public void deleteProgressByField(Map<String, Object> fields) {
        Session session = getSessionFactory().getCurrentSession();
        Class<IngestionProgress> entityClz = getEntityClass();
        StringBuilder sb = new StringBuilder();
        for (String column : fields.keySet()) {
            sb.append(column + " = :" + column + " and ");
        }
        String queryStr = String.format("delete from %s where " + sb.toString().substring(0, sb.length() - 4),
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        for (String column : fields.keySet()) {
            if (fields.get(column) instanceof ProgressStatus) {
                query.setParameter(column, fields.get(column).toString());
            } else {
                query.setParameter(column, fields.get(column));
            }
        }
        query.executeUpdate();
    }

    @Override
    public boolean isIngestionTriggered(Ingestion ingestion) {
        Session session = getSessionFactory().getCurrentSession();
        Class<IngestionProgress> entityClz = getEntityClass();
        String queryStr = String
                .format("select 1 from %s where IngestionId = :ingestionId AND TriggeredBy = :triggeredBy AND LastestStatusUpdateTime >= :latestScheduledTime",
                        entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("ingestionId", ingestion.getPid());
        query.setParameter("triggeredBy", PropDataConstants.SCAN_SUBMITTER);
        Date latestScheduledTime = CronUtils.getPreviousFireTime(ingestion.getCronExpression()).toDate();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        log.debug("Latest scheduled time: " + latestScheduledTime == null ? "null" : df.format(latestScheduledTime)
                + " for ingestion " + ingestion.toString());
        query.setParameter("latestScheduledTime", latestScheduledTime);
        if (CollectionUtils.isEmpty(query.list())) {
            return true;
        } else {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<IngestionProgress> getRetryFailedProgresses() {
        Session session = getSessionFactory().getCurrentSession();
        Class<IngestionProgress> progressEntityClz = IngestionProgress.class;
        String queryStr = String
                .format("from %s progress where progress.status = :status and progress.retries < progress.ingestion.newJobMaxRetry",
                        progressEntityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("status", ProgressStatus.FAILED);
        return query.list();
    }

    @Override
    public boolean isDuplicateProgress(IngestionProgress progress) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Ingestion> ingestionEntityClz = Ingestion.class;
        Class<IngestionProgress> progressEntityClz = IngestionProgress.class;
        String queryStr = String
                .format("select 1 from %s lhs, %s rhs where lhs.IngestionId = rhs.PID and lhs.Destination = :destination and lhs.Status != :finishStatus and not (lhs.Status = :failedStatus and lhs.Retries >= rhs.NewJobMaxRetry)",
                        progressEntityClz.getSimpleName(), ingestionEntityClz.getSimpleName());
        Query query = session.createSQLQuery(queryStr);
        query.setParameter("destination", progress.getDestination());
        query.setParameter("finishStatus", ProgressStatus.FINISHED.toString());
        query.setParameter("failedStatus", ProgressStatus.FAILED.toString());
        return !CollectionUtils.isEmpty(query.list());
    }
}
