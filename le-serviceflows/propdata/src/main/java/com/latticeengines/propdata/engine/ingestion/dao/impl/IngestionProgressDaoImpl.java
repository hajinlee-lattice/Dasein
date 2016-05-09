package com.latticeengines.propdata.engine.ingestion.dao.impl;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.manage.Ingestion;
import com.latticeengines.domain.exposed.propdata.manage.IngestionProgress;
import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.propdata.core.PropDataConstants;
import com.latticeengines.propdata.core.util.CronUtils;
import com.latticeengines.propdata.engine.ingestion.dao.IngestionProgressDao;

import reactor.util.CollectionUtils;

@Component("ingestionProgressDao")
public class IngestionProgressDaoImpl extends
        BaseDaoWithAssignedSessionFactoryImpl<IngestionProgress> implements IngestionProgressDao {
    private static final Log log = LogFactory.getLog(IngestionProgressDaoImpl.class);

    @Override
    protected Class<IngestionProgress> getEntityClass() {
        return IngestionProgress.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<IngestionProgress> getProgressesByField(Map<String, Object> fields) {
        Session session = getSessionFactory().getCurrentSession();
        Class<IngestionProgress> entityClz = getEntityClass();
        StringBuilder sb = new StringBuilder();
        for (String column : fields.keySet()) {
            sb.append(column + " = :" + column + " and ");
        }
        String queryStr = String.format(
                "from %s where " + sb.toString().substring(0, sb.length() - 4),
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        for (String column : fields.keySet()) {
            if (fields.get(column) instanceof ProgressStatus) {
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
        String queryStr = String.format(
                "delete from %s where " + sb.toString().substring(0, sb.length() - 4),
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
        String queryStr = String.format(
                "select 1 from %s where IngestionId = :ingestionId AND TriggeredBy = :triggeredBy AND LastestStatusUpdateTime >= :lastestScheduledTime",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("ingestionId", ingestion.getPid());
        query.setParameter("triggeredBy", PropDataConstants.SCAN_SUBMITTER);
        Date lastestScheduledTime;
        try {
            lastestScheduledTime = CronUtils.getPreviousFireTime(ingestion.getCronExpression());
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            log.debug("Latest scheduled time: " + lastestScheduledTime == null ? "null"
                    : df.format(lastestScheduledTime) + " for ingestion " + ingestion.toString());
        } catch (ParseException e) {
            throw new RuntimeException(
                    "Failed to parse cron expression " + ingestion.getCronExpression());
        }
        query.setParameter("lastestScheduledTime", lastestScheduledTime);
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
        String queryStr = String.format(
                "from %s progress where progress.status = :status and progress.retries < progress.ingestion.newJobMaxRetry",
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
        String queryStr = String.format(
                "select 1 from %s lhs, %s rhs where lhs.IngestionId = rhs.PID and lhs.Destination = :destination and lhs.Status != :finishStatus and not (lhs.Status = :failedStatus and lhs.Retries >= rhs.NewJobMaxRetry)",
                progressEntityClz.getSimpleName(), ingestionEntityClz.getSimpleName());
        Query query = session.createSQLQuery(queryStr);
        query.setParameter("destination", progress.getDestination());
        query.setParameter("finishStatus", ProgressStatus.FINISHED.toString());
        query.setParameter("failedStatus", ProgressStatus.FAILED.toString());
        return !CollectionUtils.isEmpty(query.list());
    }
}
