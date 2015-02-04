package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.hibernate.Hibernate;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.PredictorElement;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.ModelSummaryDao;
import com.latticeengines.pls.dao.PredictorDao;
import com.latticeengines.pls.dao.PredictorElementDao;
import com.latticeengines.pls.dao.TenantDao;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.security.TicketAuthenticationToken;

@Component("modelSummaryEntityMgr")
public class ModelSummaryEntityMgrImpl extends BaseEntityMgrImpl<ModelSummary> implements ModelSummaryEntityMgr {

    @Autowired
    private ModelSummaryDao modelSummaryDao;

    @Autowired
    private TenantDao tenantDao;

    @Autowired
    private PredictorDao predictorDao;

    @Autowired
    private PredictorElementDao predictorElementDao;
    
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
    public ModelSummary findByModelId(String modelId) {
        ModelSummary summary = modelSummaryDao.findByModelId(modelId);
        
        if (summary.getTenantId() != getTenantId()) {
            return null;
        }
        inflatePredictors(summary);
        return summary; 
    }
    
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ModelSummary> findAll() {
        return super.findAll();
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
        // By the time this method is invoked, the aspec joinpoint in MultiTenantEntityMgrAspect would
        // have been invoked, and any exceptions with respect to nulls would already
        // have been caught there, which is why there is no defensive checking here
        TicketAuthenticationToken token = (TicketAuthenticationToken) SecurityContextHolder.getContext().getAuthentication();
        return token.getSession().getTenant().getPid();
    }

}
