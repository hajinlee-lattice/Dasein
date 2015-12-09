package com.latticeengines.pls.entitymanager.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Hibernate;
import org.hibernate.proxy.HibernateProxy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.annotations.VisibleForTesting;
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
import com.latticeengines.domain.exposed.pls.PredictorStatus;
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

    @Override
    public BaseDao<ModelSummary> getDao() {
        return modelSummaryDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(ModelSummary summary) {
        Tenant tenant = summary.getTenant();
        Long tenantId = tenant.getPid();
        tenantDao.createOrUpdate(tenant);
        KeyValue details = summary.getDetails();

        if (details != null) {
            if (details.getTenantId() == null) {
                details.setTenantId(tenantId);
            }
            keyValueDao.create(details);
        }

        modelSummaryDao.create(summary);

        for (Predictor predictor : summary.getPredictors()) {
            predictor.setTenantId(tenantId);
            predictor.setModelSummary(summary);
            predictorDao.create(predictor);

            for (PredictorElement el : predictor.getPredictorElements()) {
                el.setPredictor(predictor);
                el.setTenantId(tenantId);
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

    private Long getTenantId() {
        // By the time this method is invoked, the aspect joinpoint in
        // MultiTenantEntityMgrAspect would
        // have been invoked, and any exceptions with respect to nulls would
        // already
        // have been caught there, which is why there is no defensive checking
        // here
        TicketAuthenticationToken token = (TicketAuthenticationToken) SecurityContextHolder.getContext()
                .getAuthentication();
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
        // the KeyValue table so we cannot create a foreign key from KEY_VALUE
        // to
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
    public ModelSummary findByModelId(String modelId, boolean returnRelational, boolean returnDocument,
            boolean validOnly) {
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
            log.warn(String.format("Summary tenant id = %d, Security context tenant id = %d", summaryTenantId,
                    secCtxTenantId));
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
    public ModelSummary findByApplicationId(String applicationId) {
        ModelSummary summary = modelSummaryDao.findByApplicationId(applicationId);
        if (summary != null) {
            inflateDetails(summary);
        }
        if (summary != null) {
            inflatePredictors(summary);
        }
        return summary;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public ModelSummary retrieveByModelIdForInternalOperations(String modelId) {
        ModelSummary summary = modelSummaryDao.findByModelId(modelId);
        if (summary != null) {
            inflateDetails(summary);
        }
        if (summary != null) {
            inflatePredictors(summary);
        }
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

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Predictor> findAllPredictorsByModelId(String modelId) {
        if (modelId == null) {
            throw new NullPointerException("ModelId should not be null when finding all the predictors.");
        }
        ModelSummary summary = findByModelId(modelId, true, false, true);
        return summary == null ? null : summary.getPredictors();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Predictor> findPredictorsUsedByBuyerInsightsByModelId(String modelId) {
        if (modelId == null) {
            throw new NullPointerException("ModelId should not be null when finding the predictors For BuyerInsights.");
        }
        ModelSummary summary = findByModelId(modelId, true, false, true);
        if (summary == null) {
            return null;
        }
        List<Predictor> allPredictors = summary.getPredictors();
        List<Predictor> predictorForBi = new ArrayList<Predictor>();
        for (Predictor predictor : allPredictors) {
            if (predictor.getUsedForBuyerInsights()) {
                predictorForBi.add(predictor);
            }
        }
        return predictorForBi;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updatePredictors(List<Predictor> predictors, AttributeMap attrMap) {

        if (predictors == null) {
            throw new NullPointerException("Predictors should not be null when updating the predictors");
        }
        if (attrMap == null) {
            throw new NullPointerException("Attribute Map should not be null when updating the predictors");
        }

        List<String> missingPredictors = getMissingPredictors(predictors, attrMap);
        if (missingPredictors.size() != 0) {
            throw new LedpException(LedpCode.LEDP_18052,
                    missingPredictors.toArray(new String[missingPredictors.size()]));
        }

        for (Predictor predictor : predictors) {
            String predictorName = predictor.getName();
            if (predictorName == null) {
                throw new NullPointerException("predictorName should not be null.");
            }
            if (attrMap.containsKey(predictorName)) {
                PredictorStatus updateStatus = PredictorStatus.getStatusByName(attrMap.get(predictorName));
                switch (updateStatus) {
                case NOT_USED_FOR_BUYER_INSIGHTS:
                    predictor.setUsedForBuyerInsights(PredictorStatus.NOT_USED_FOR_BUYER_INSIGHTS.getStatus());
                    break;
                case USED_FOR_BUYER_INSIGHTS:
                    predictor.setUsedForBuyerInsights(PredictorStatus.USED_FOR_BUYER_INSIGHTS.getStatus());
                    break;
                default:
                    log.warn("Invalid input for updating predictor status.");
                    break;
                }
                predictorDao.update(predictor);
            }
        }
    }

    @VisibleForTesting
    List<String> getMissingPredictors(List<Predictor> predictors, AttributeMap attrMap) {

        if (predictors == null) {
            throw new NullPointerException("Predictors should not be null when updating the predictors");
        }
        if (attrMap == null) {
            throw new NullPointerException("Attribute Map should not be null when updating the predictors");
        }

        List<String> predictorNameList = new ArrayList<String>();
        for (Predictor predictor : predictors) {
            predictorNameList.add(predictor.getName());
        }
        List<String> missingNameList = new ArrayList<String>();
        Set<String> updatePredictorNameSet = attrMap.keySet();
        if (!predictorNameList.containsAll(updatePredictorNameSet)) {
            for (String updatePredictorName : updatePredictorNameSet) {
                if (!predictorNameList.contains(updatePredictorName)) {
                    missingNameList.add(updatePredictorName);
                }
            }
        }
        return missingNameList;
    }
}
