package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Hibernate;
import org.hibernate.proxy.HibernateProxy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.KeyValue;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.PredictorElement;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.KeyValueDao;
import com.latticeengines.pls.dao.ModelSummaryDao;
import com.latticeengines.pls.dao.PredictorDao;
import com.latticeengines.pls.dao.PredictorElementDao;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.security.exposed.TicketAuthenticationToken;
import com.latticeengines.security.exposed.dao.TenantDao;

@Component("modelSummaryEntityMgr")
public class ModelSummaryEntityMgrImpl extends BaseEntityMgrImpl<ModelSummary> implements ModelSummaryEntityMgr {

    private static final Log log = LogFactory.getLog(ModelSummaryEntityMgrImpl.class);

    @Autowired
    private KeyValueDao keyValueDao;

    @Autowired
    private PredictorDao predictorDao;

    @Autowired
    private PredictorElementDao predictorElementDao;

    @Autowired
    private ModelSummaryDao modelSummaryDao;

    @Autowired
    private TenantDao tenantDao;

//    @Autowired
//    private SessionFactory sessionFactory;

    @Override
    public BaseDao<ModelSummary> getDao() {
        return modelSummaryDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(ModelSummary summary) {
        Tenant tenant = summary.getTenant();
        tenantDao.createOrUpdate(tenant);
        KeyValue details = summary.getDetails();

        if (details != null) {
            if (details.getTenantId() == null) {
                details.setTenantId(tenant.getPid());
            }
            keyValueDao.create(details);
        }

        modelSummaryDao.create(summary);

        for (Predictor predictor : summary.getPredictors()) {
            predictorDao.create(predictor);

            for (PredictorElement el : predictor.getPredictorElements()) {
                predictorElementDao.create(el);
            }
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ModelSummary> getAll() {
        return super.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ModelSummary> findAll() {
        return super.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ModelSummary> findAllValid() {
        return modelSummaryDao.findAllValid();
    }

    private void inflateDetails(ModelSummary summary) {
        KeyValue kv = summary.getDetails();
        Hibernate.initialize(kv);
        if (kv instanceof HibernateProxy) {
            kv = (KeyValue) ((HibernateProxy) kv).getHibernateLazyInitializer().getImplementation();
            summary.setDetails(kv);
        }
        kv.setTenantId(summary.getTenantId());
    }

    private void inflatePredictors(ModelSummary summary) {
        List<Predictor> predictors = summary.getPredictors();
        Hibernate.initialize(predictors);
        if (predictors.size() > 0) {
            for (Predictor predictor : predictors) {
                Hibernate.initialize(predictor.getPredictorElements());
            }

        }
    }

    private Long getTenantId()  {
        // By the time this method is invoked, the aspect joinpoint in MultiTenantEntityMgrAspect would
        // have been invoked, and any exceptions with respect to nulls would already
        // have been caught there, which is why there is no defensive checking here
        TicketAuthenticationToken token = (TicketAuthenticationToken) SecurityContextHolder.getContext().getAuthentication();
        return token.getSession().getTenant().getPid();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteByModelId(String modelId) {
        ModelSummary summary = findValidByModelId(modelId);

        if (summary == null) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }
        super.delete(summary);

        // We need to have a separate call to delete from the KeyValue table
        // because the idea is that multiple entities should be able to use
        // the KeyValue table so we cannot create a foreign key from KEY_VALUE to
        // the owning entity to get the delete cascade effect.
        // Instead a delete of a model summary needs to reach into the KEY_VALUE
        // table, and delete the associated KeyValue instance
        Long detailsPid = summary.getDetails().getPid();

        if (detailsPid == null) {
            log.warn("No details related to the model summary with model id = " + modelId);
        }

        KeyValue kv = keyValueDao.findByKey(KeyValue.class, detailsPid);
        if (!kv.getTenantId().equals(summary.getTenantId())) {
            log.error("Model and detail tenants are different!");
        }
        keyValueDao.delete(kv);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateModelSummary(ModelSummary modelSummary, AttributeMap attrMap) {
        String modelId = modelSummary.getId();

        if (modelId == null) {
            throw new LedpException(LedpCode.LEDP_18008, new String[] { "Id" });
        }

        // If it's a status update, then allow for getting deleted models
        boolean statusUpdate = attrMap.containsKey("Status");

        ModelSummary summary = findByModelId(modelId, false, true, !statusUpdate);

        if (summary == null) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }

        // Update status
        updateStatus(summary, attrMap);

        // Update name
        updateName(summary, attrMap);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateStatusByModelId(String modelId, ModelSummaryStatus status) {
        ModelSummary summary = findByModelId(modelId, false, true, false);

        if (summary == null) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }

        if (status == ModelSummaryStatus.DELETED && summary.getStatus() == ModelSummaryStatus.ACTIVE) {
            throw new LedpException(LedpCode.LEDP_18021);
        }
        if (status == ModelSummaryStatus.ACTIVE && summary.getStatus() == ModelSummaryStatus.DELETED) {
            throw new LedpException(LedpCode.LEDP_18024);
        }
        summary.setStatus(status);
        super.update(summary);
    }

    private void updateStatus(ModelSummary summary, AttributeMap attrMap) {
        String status = attrMap.get("Status");
        if (status == null) {
            return;
        }
        updateStatusByModelId(summary.getId(), ModelSummaryStatus.getByStatusCode(status));
    }

    private void updateName(ModelSummary summary, AttributeMap attrMap) {
        String name = attrMap.get("Name");
        if (name != null) {
            if (newModelNameIsValid(summary, name)) {
                summary.setName(name);
            }

            super.update(summary);
        }
    }

    private boolean newModelNameIsValid(ModelSummary summary, String name) {
        if (name == null) {
            throw new LedpException(LedpCode.LEDP_18008, new String[] { "Name" });
        }
        String oldName = summary.getName();
        if (name.equals(oldName)) {
            return true;
        }

        if (modelSummaryDao.findByModelName(name) != null) {
            throw new LedpException(LedpCode.LEDP_18014, new String[] { name });
        }
        return true;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ModelSummary findByModelId(String modelId, boolean returnRelational, boolean returnDocument, boolean validOnly) {
        ModelSummary summary = null;
        if (validOnly) {
            summary = modelSummaryDao.findValidByModelId(modelId);
        } else {
            summary = modelSummaryDao.findByModelId(modelId);
        }

        if (summary == null) {
            return null;
        }
        Long summaryTenantId = summary.getTenantId();
        Long secCtxTenantId = getTenantId();
        if (summaryTenantId == null //
                || secCtxTenantId == null //
                || summaryTenantId.longValue() != secCtxTenantId.longValue()) {
            log.warn(String.format("Summary tenant id = %d, Security context tenant id = %d", summaryTenantId, secCtxTenantId));
            return null;
        }
        if (returnRelational) {
            inflatePredictors(summary);
        }
        if (returnDocument) {
            inflateDetails(summary);
        }

        return summary;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public ModelSummary retrieveByModelIdForInternalOperations(String modelId) {
        ModelSummary summary = modelSummaryDao.findByModelId(modelId);
        if (summary != null) inflateDetails(summary);
        if (summary != null) inflatePredictors(summary);
        return summary;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ModelSummary findValidByModelId(String modelId) {
        return findByModelId(modelId, false, true, true);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ModelSummary getByModelId(String modelId) {
        return modelSummaryDao.findByModelId(modelId);
    }

    public void manufactureSecurityContextForInternalAccess(Tenant tenant) {
        TicketAuthenticationToken auth = new TicketAuthenticationToken(null, "x.y");
        Session session = new Session();
        session.setTenant(tenant);
        auth.setSession(session);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

}
