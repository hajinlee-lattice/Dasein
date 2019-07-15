package com.latticeengines.apps.lp.entitymgr.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.hibernate.Hibernate;
import org.hibernate.proxy.HibernateProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.core.annotation.ClearModelSummary;
import com.latticeengines.apps.lp.cache.ModelSummaryCacheWriter;
import com.latticeengines.apps.lp.dao.ModelSummaryDao;
import com.latticeengines.apps.lp.dao.ModelSummaryProvenancePropertyDao;
import com.latticeengines.apps.lp.dao.PredictorDao;
import com.latticeengines.apps.lp.dao.PredictorElementDao;
import com.latticeengines.apps.lp.entitymgr.ModelSummaryEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.dao.KeyValueDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryProvenanceProperty;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.PredictorElement;
import com.latticeengines.domain.exposed.pls.PredictorStatus;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.KeyValue;


@Component("modelSummaryEntityMgr")
public class ModelSummaryEntityMgrImpl extends BaseEntityMgrImpl<ModelSummary> implements ModelSummaryEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(ModelSummaryEntityMgrImpl.class);

    @Inject
    private ModelSummaryDao dao;

    @Inject
    private KeyValueDao keyValueDao;

    @Inject
    private PredictorDao predictorDao;

    @Inject
    private PredictorElementDao predictorElementDao;

    @Inject
    private ModelSummaryProvenancePropertyDao modelSummaryProvenancePropertyDao;

    @Inject
    private ModelSummaryCacheWriter modelSummaryCacheWriter;

    @Override
    public BaseDao<ModelSummary> getDao() {
        return dao;
    }

    @ClearModelSummary
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public void delete(ModelSummary modelSummary) {
        super.delete(modelSummary);
    }

    @ClearModelSummary
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public void update(ModelSummary modelSummary) {
        super.update(modelSummary);
    }

    @ClearModelSummary
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public void createOrUpdate(ModelSummary modelSummary) {
        super.createOrUpdate(modelSummary);
    }

    @ClearModelSummary
    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void create(ModelSummary summary) {
        Tenant tenant = summary.getTenant();
        Long tenantId = tenant.getPid();
        KeyValue details = summary.getDetails();

        if (details != null) {
            if (details.getTenantId() == null) {
                details.setTenantId(tenantId);
            }
            keyValueDao.create(details);
        }
        summary.setLastUpdateTime(System.currentTimeMillis());
        dao.create(summary);

        for (ModelSummaryProvenanceProperty provenanceProperty : summary.getModelSummaryProvenanceProperties()) {
            provenanceProperty.setModelSummary(summary);
            log.info(String.format("creating model summary provenance with name: %s, value: %s",
                    provenanceProperty.getOption(), provenanceProperty.getValue()));
            modelSummaryProvenancePropertyDao.create(provenanceProperty);
        }

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
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ModelSummary> getAll() {
        return super.findAll();
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<String> getAllModelSummaryIds() {
        return dao.getAllModelSummaryIds();
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ModelSummary> getAllByTenant(Tenant tenant) {
        return dao.getAllByTenant(tenant);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ModelSummary> findAll() {
        return super.findAll();
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ModelSummary> findAllValid() {
        return dao.findAllValid();
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ModelSummary> findAllActive() {
        return dao.findAllActive();
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public int findTotalCount(long lastUpdateTime, boolean considerAllStatus) {
        return dao.findTotalCount(lastUpdateTime, considerAllStatus);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ModelSummary> findPaginatedModels(long lastUpdateTime, boolean considerAllStatus, int offset,
                                                  int maximum) {
        List<ModelSummary> models = dao.findPaginatedModels(lastUpdateTime, considerAllStatus, offset,
                maximum);

        if (!CollectionUtils.isEmpty(models)) {
            for (ModelSummary model : models) {
                inflatePredictors(model);
            }
        }

        return models;
    }

    private void inflateDetails(ModelSummary summary) {
        KeyValue kv = summary.getDetails();
        Hibernate.initialize(kv);
        if (kv instanceof HibernateProxy) {
            kv = (KeyValue) ((HibernateProxy) kv).getHibernateLazyInitializer().getImplementation();
            summary.setDetails(kv);
        }
        kv.setTenantId(summary.getTenantId());

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode details = objectMapper.readTree(summary.getDetails().getPayload());
            JsonNode provenance = details.get("EventTableProvenance");
            if (provenance != null) {
                if (provenance.has("Predefined_ColumnSelection_Name")) {
                    String predefinedSelectionName = provenance.get("Predefined_ColumnSelection_Name").asText();
                    ColumnSelection.Predefined predefined = ColumnSelection.Predefined.fromName(predefinedSelectionName);
                    summary.setPredefinedSelection(predefined);
                    if (provenance.has("Predefined_ColumnSelection_Version")) {
                        String predefinedSelectionVersion = provenance.get("Predefined_ColumnSelection_Version")
                                .asText();
                        summary.setPredefinedSelectionVersion(predefinedSelectionVersion);
                    }
                } else if (provenance.has("Customized_ColumnSelection")) {
                    ColumnSelection selection = objectMapper.treeToValue(provenance.get("Customized_ColumnSelection"),
                            ColumnSelection.class);
                    summary.setCustomizedColumnSelection(selection);
                }
                if (provenance.has("Data_Cloud_Version")) {
                    String dataCloudVersion = provenance.get("Data_Cloud_Version").asText();
                    summary.setDataCloudVersion(dataCloudVersion);
                }
                if (provenance.has("Exclude_Propdata_Columns")) {
                    String skipMatch = provenance.get("Exclude_Propdata_Columns").asText();
                    summary.setMatch("false".equals(skipMatch));
                }
            }
            JsonNode meanNode = details.get("CrossValidatedMeanOfModelAccuracy");
            if (meanNode != null) {
                Double crossValidatedMean = meanNode.asDouble();
                summary.setCrossValidatedMean(crossValidatedMean);
            }
            JsonNode stdNode = details.get("CrossValidatedStdOfModelAccuracy");
            if (stdNode != null) {
                Double crossValidatedStd = stdNode.asDouble();
                summary.setCrossValidatedStd(crossValidatedStd);
            }

        } catch (IOException e) {
            log.error("Failed to parse model details KeyValue", e);
        }

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
        return MultiTenantContext.getTenant().getPid();
    }

    @ClearModelSummary
    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void deleteByModelId(String modelId) {
        ModelSummary summary = findValidByModelId(modelId);

        if (summary == null) {
            throw new LedpException(LedpCode.LEDP_18007, new String[]{modelId});
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

    @ClearModelSummary
    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void updateModelSummary(ModelSummary modelSummary, AttributeMap attrMap) {
        String modelId = modelSummary.getId();

        if (modelId == null) {
            throw new LedpException(LedpCode.LEDP_18008, new String[]{"Id"});
        }

        // If it's a status update, then allow for getting deleted models
        boolean statusUpdate = attrMap.containsKey("Status");

        ModelSummary summary = findByModelId(modelId, false, true, !statusUpdate);

        if (summary == null) {
            throw new LedpException(LedpCode.LEDP_18007, new String[]{modelId});
        }

        // Update status
        updateStatus(summary, attrMap);

        // Update name
        updateDisplayName(summary, attrMap);
    }

    @ClearModelSummary
    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void updateStatusByModelId(String modelId, ModelSummaryStatus status) {
        ModelSummary summary = findByModelId(modelId, false, true, false);

        if (summary == null) {
            throw new LedpException(LedpCode.LEDP_18007, new String[]{modelId});
        }

        if (status == ModelSummaryStatus.DELETED && summary.getStatus() == ModelSummaryStatus.ACTIVE) {
            throw new LedpException(LedpCode.LEDP_18021);
        }
        if (status == ModelSummaryStatus.ACTIVE && summary.getStatus() == ModelSummaryStatus.DELETED) {
            throw new LedpException(LedpCode.LEDP_18024);
        }
        summary.setStatus(status);
        // need to set it explicitly as @PreUpdate is not being called
        summary.setLastUpdateTime(System.currentTimeMillis());
        super.update(summary);
    }

    private void updateStatus(ModelSummary summary, AttributeMap attrMap) {
        String status = attrMap.get(ModelSummary.STATUS);
        if (status == null) {
            return;
        }
        updateStatusByModelId(summary.getId(), ModelSummaryStatus.getByStatusCode(status));
    }

    private void updateDisplayName(ModelSummary summary, AttributeMap attrMap) {
        String displayName = attrMap.get(ModelSummary.DISPLAY_NAME);
        if (displayName != null) {
            summary.setDisplayName(displayName);

            // need to set it explicitly as @PreUpdate is not being called
            summary.setLastUpdateTime(System.currentTimeMillis());
            super.update(summary);
        }
    }

    @ClearModelSummary
    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW)
    public void updateLastUpdateTime(ModelSummary summary) {
        summary.setLastUpdateTime(System.currentTimeMillis());
        super.update(summary);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ModelSummary findByModelId(String modelId, boolean returnRelational, boolean returnDocument,
                                      boolean validOnly) {
        ModelSummary summary;
        if (validOnly) {
            summary = dao.findValidByModelId(modelId);
        } else {
            summary = dao.findByModelId(modelId);
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
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ModelSummary findByApplicationId(String applicationId) {
        ModelSummary summary = dao.findByApplicationId(applicationId);
        if (summary != null) {
            inflateDetails(summary);
        }
        if (summary != null) {
            inflatePredictors(summary);
        }
        return summary;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ModelSummary> getModelSummariesByApplicationId(String applicationId) {
        List<ModelSummary> modelSummaries = dao.getModelSummariesByApplicationId(applicationId);

        for (ModelSummary modelSummary : modelSummaries) {
            modelSummary.setPredictors(new ArrayList<>());
            modelSummary.setDetails(null);
        }
        return modelSummaries;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public ModelSummary retrieveByModelIdForInternalOperations(String modelId) {
        ModelSummary summary = dao.findByModelId(modelId);
        if (summary != null) {
            inflateDetails(summary);
        }
        if (summary != null) {
            inflatePredictors(summary);
        }
        return summary;
    }

    @ClearModelSummary
    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void deleteByModelIdForInternalOperations(String modelId) {
        ModelSummary summary = retrieveByModelIdForInternalOperations(modelId);
        if (summary.getDetails() != null) {
            keyValueDao.delete(summary.getDetails());
        }
        if (summary.getPredictors() != null) {
            for (Predictor predictor : summary.getPredictors()) {
                predictorDao.delete(predictor);
            }
        }
        dao.delete(summary);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ModelSummary findValidByModelId(String modelId) {
        return findByModelId(modelId, false, true, true);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ModelSummary getByModelId(String modelId) {
        return dao.findByModelId(modelId);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Predictor> findAllPredictorsByModelId(String modelId) {
        if (modelId == null) {
            throw new NullPointerException("ModelId should not be null when finding all the predictors.");
        }
        ModelSummary summary = findByModelId(modelId, true, false, true);
        return summary == null ? null : summary.getPredictors();
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Predictor> findPredictorsUsedByBuyerInsightsByModelId(String modelId) {
        if (modelId == null) {
            throw new NullPointerException("ModelId should not be null when finding the predictors For BuyerInsights.");
        }
        ModelSummary summary = findByModelId(modelId, true, false, true);
        if (summary == null) {
            return null;
        }
        List<Predictor> allPredictors = summary.getPredictors();
        List<Predictor> predictorForBi = new ArrayList<>();
        for (Predictor predictor : allPredictors) {
            if (predictor.getUsedForBuyerInsights()) {
                predictorForBi.add(predictor);
            }
        }
        return predictorForBi;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
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

        List<String> predictorNameList = new ArrayList<>();
        for (Predictor predictor : predictors) {
            predictorNameList.add(predictor.getName());
        }
        List<String> missingNameList = new ArrayList<>();
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

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ModelSummary getByModelNameInTenant(String modelName, Tenant tenant) {
        return dao.getByModelNameInTenant(modelName, tenant);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ModelSummary> getModelSummariesModifiedWithinTimeFrame(long timeframe) {
        List<ModelSummary> modelSummaryList = dao.getModelSummariesModifiedWithinTimeFrame(timeframe);
        modelSummaryList.forEach(this::inflateDetails);
        return modelSummaryList;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public boolean hasBucketMetadata(String modelId) {
        return dao.hasBucketMetadata(modelId);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ModelSummary> findModelSummariesByIds(Set<String> ids) {
        return dao.findModelSummariesByIds(ids);
    }

    @ClearModelSummary
    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void updateLastUpdateTime(String modelGuid) {
        ModelSummary summary = dao.findByModelId(modelGuid);
        if (summary == null) {
            throw new LedpException(LedpCode.LEDP_18007, new String[]{modelGuid});
        }
        summary.setLastUpdateTime(System.currentTimeMillis());
        dao.merge(summary);
    }

}
