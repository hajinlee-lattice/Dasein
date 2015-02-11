package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Hibernate;
import org.hibernate.SessionFactory;
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
import com.latticeengines.domain.exposed.pls.KeyValue;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.PredictorElement;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.KeyValueDao;
import com.latticeengines.pls.dao.ModelSummaryDao;
import com.latticeengines.pls.dao.PredictorDao;
import com.latticeengines.pls.dao.PredictorElementDao;
import com.latticeengines.pls.dao.TenantDao;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.security.TicketAuthenticationToken;

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

    @Autowired
    private SessionFactory sessionFactory;

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
    public List<ModelSummary> findAll() {
        return super.findAll();
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
        ModelSummary summary = findByModelId(modelId);
        
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
        if (kv.getTenantId() != summary.getTenantId()) {
            log.error("Model and detail tenants are different!");
        }
        keyValueDao.delete(kv);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateModelSummary(ModelSummary modelSummary) {
        String modelId = modelSummary.getId();
        
        if (modelId == null) {
            throw new LedpException(LedpCode.LEDP_18008, new String[] { "Id" });
        }
        ModelSummary summary = findByModelId(modelSummary.getId());
        
        if (summary == null) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }
        
        // Support renaming only
        String name = modelSummary.getName();
        if (name == null) {
            throw new LedpException(LedpCode.LEDP_18008, new String[] { "Name" });
        }
        
        summary.setName(name);
        super.update(summary);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ModelSummary findByModelId(String modelId, boolean returnRelational, boolean returnDocument) {
        ModelSummary summary = modelSummaryDao.findByModelId(modelId);
        
        if (summary == null) {
            return null;
        }

        if (summary.getTenantId() != getTenantId()) {
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
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ModelSummary findByModelId(String modelId) {
        return findByModelId(modelId, false, true);
    }
}
