package com.latticeengines.apps.lp.entitymgr.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.core.repository.writer.ModelSummaryWriterRepository;
import com.latticeengines.apps.lp.dao.ModelSummaryDao;
import com.latticeengines.apps.lp.dao.ModelSummaryProvenancePropertyDao;
import com.latticeengines.apps.lp.dao.PredictorDao;
import com.latticeengines.apps.lp.dao.PredictorElementDao;
import com.latticeengines.apps.lp.entitymgr.ModelSummaryEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.dao.KeyValueDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryProvenanceProperty;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.PredictorElement;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.KeyValue;


@Component("modelSummaryEntityMgr")
public class ModelSummaryEntityMgrImpl extends BaseEntityMgrRepositoryImpl<ModelSummary, Long>
        implements ModelSummaryEntityMgr {

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
    private ModelSummaryWriterRepository repository;

    @Override
    public BaseJpaRepository<ModelSummary, Long> getRepository() {
        return repository;
    }

    @Override
    public BaseDao<ModelSummary> getDao() {
        return dao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
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
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateLastUpdateTime(String modelGuid) {
        ModelSummary summary = repository.findById(modelGuid);
        if (summary != null) {
            summary.setLastUpdateTime(System.currentTimeMillis());
            dao.merge(summary);
        }
    }

}
