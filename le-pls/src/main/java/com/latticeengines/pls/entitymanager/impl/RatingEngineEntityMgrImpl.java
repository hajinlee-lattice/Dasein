package com.latticeengines.pls.entitymanager.impl;

import java.util.Date;
import java.util.List;
import java.util.Set;

import org.hibernate.Hibernate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.RatingEngineDao;
import com.latticeengines.pls.entitymanager.RatingEngineEntityMgr;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@Component("ratingEngineEntityMgr")
public class RatingEngineEntityMgrImpl extends BaseEntityMgrImpl<RatingEngine> implements RatingEngineEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineEntityMgrImpl.class);

    @Autowired
    private RatingEngineDao ratingEngineDao;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public BaseDao<RatingEngine> getDao() {
        return ratingEngineDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<RatingEngine> findAll() {
        return super.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public RatingEngine findById(String id) {
        return findById(id, false, false, false);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteById(String id) {
        RatingEngine ratingEngine = findById(id);
        if (ratingEngine == null) {
            throw new NullPointerException(String.format("RatingEngine with id %s cannot be found", id));
        }
        super.delete(ratingEngine);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteRatingEngine(RatingEngine ratingEngine) {
        if (ratingEngine == null) {
            throw new NullPointerException("RatingEngine cannot be found");
        }
        super.delete(ratingEngine);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public RatingEngine createOrUpdateRatingEngine(RatingEngine ratingEngine, String tenantId) {

        if (ratingEngine.getId() == null) { // create a new Rating Engine
            ratingEngine.setId(RatingEngine.generateIdStr());
            createNewRatingEngine(ratingEngine, tenantId);
            return ratingEngine;
        } else {
            RatingEngine retrievedRatingEngine = findById(ratingEngine.getId());
            if (retrievedRatingEngine == null) {
                log.warn(String.format("Rating Engine with id %s for tenant %s cannot be found", ratingEngine.getId(),
                        tenantId));
                createNewRatingEngine(ratingEngine, tenantId);
                return ratingEngine;
            } else { // update an existing one by updating the delta passed from
                     // front end
                updateExistingRatingEngine(retrievedRatingEngine, ratingEngine, tenantId);
                inflateRatingModels(retrievedRatingEngine);
                return retrievedRatingEngine;
            }
        }
    }

    private void updateExistingRatingEngine(RatingEngine retrievedRatingEngine, RatingEngine ratingEngine,
            String tenantId) {
        log.info(String.format("Updating existing rating engine with id %s for tenant %s", ratingEngine.getId(),
                tenantId));
        if (ratingEngine.getDisplayName() != null) {
            retrievedRatingEngine.setDisplayName(ratingEngine.getDisplayName());
        }
        if (ratingEngine.getNote() != null) {
            retrievedRatingEngine.setNote(ratingEngine.getNote());
        }
        if (ratingEngine.getSegment() != null) {
            retrievedRatingEngine.setSegment(ratingEngine.getSegment());
        }
        if (ratingEngine.getStatus() != null) {
            retrievedRatingEngine.setStatus(ratingEngine.getStatus());
        }
        ratingEngineDao.update(retrievedRatingEngine);
    }

    private void createNewRatingEngine(RatingEngine ratingEngine, String tenantId) {
        log.info(String.format("Creating a new Rating Engine entity with the id of %s for tenant %s",
                ratingEngine.getId(), tenantId));
        Tenant tenant = tenantEntityMgr.findByTenantId(tenantId);
        ratingEngine.setTenant(tenant);
        RatingEngineType type = ratingEngine.getType();
        if (type == null) {
            throw new LedpException(LedpCode.LEDP_18154, new String[] { ratingEngine.toString() });
        }
        MetadataSegment segment = ratingEngine.getSegment();
        if (segment == null || segment.getName() == null) {
            throw new LedpException(LedpCode.LEDP_18153, new String[] { ratingEngine.toString() });
        }
        switch (type) {
        case RULE_BASED:

            RuleBasedModel ruleBasedModel = new RuleBasedModel();
            ruleBasedModel.setId(RuleBasedModel.generateIdStr());
            ruleBasedModel.setRatingRule(new RatingRule());
            ruleBasedModel.setCreated(new Date());
            ruleBasedModel.setUpdated(new Date());
            ratingEngine.addRatingModel(ruleBasedModel);
            ratingEngineDao.create(ratingEngine);
            break;
        case AI_BASED:
            break;
        default:
            break;
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public RatingEngine findById(String id, boolean inflateSegment, boolean inflateRatingModels, boolean inflatePlays) {
        RatingEngine ratingEngine = ratingEngineDao.findById(id);
        if (inflateSegment) {
            inflateSegment(ratingEngine);
        }
        if (inflateRatingModels) {
            inflateRatingModels(ratingEngine);
        }
        if (inflatePlays) {
            inflatePlays(ratingEngine);
        }
        return ratingEngine;
    }

    private void inflateSegment(RatingEngine ratingEngine) {
        MetadataSegment segment = ratingEngine.getSegment();
        Hibernate.initialize(segment);
    }

    private void inflateRatingModels(RatingEngine ratingEngine) {
        Set<RatingModel> ratingModels = ratingEngine.getRatingModels();
        Hibernate.initialize(ratingModels);
    }

    private void inflatePlays(RatingEngine ratingEngine) {
        // TODO assocaite Play object with Rating Engine object
    }
}
