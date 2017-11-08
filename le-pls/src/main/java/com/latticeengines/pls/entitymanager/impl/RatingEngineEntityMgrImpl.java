package com.latticeengines.pls.entitymanager.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.traversal.impl.DepthFirstSearch;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
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
        return findAllByTypeAndStatus(null, null);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<RatingEngine> findAllByTypeAndStatus(String type, String status) {
        return ratingEngineDao.findAllByTypeAndStatus(type, status);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public RatingEngine findById(String id) {
        return ratingEngineDao.findById(id);
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
        if (MapUtils.isNotEmpty(ratingEngine.getCountsAsMap())) {
            retrievedRatingEngine.setCountsByMap(ratingEngine.getCountsAsMap());
        }
        retrievedRatingEngine.setUpdated(new Date());
        ratingEngineDao.update(retrievedRatingEngine);
    }

    private void createNewRatingEngine(RatingEngine ratingEngine, String tenantId) {
        log.info(String.format("Creating a new Rating Engine entity with the id of %s for tenant %s",
                ratingEngine.getId(), tenantId));
        Tenant tenant = tenantEntityMgr.findByTenantId(tenantId);
        ratingEngine.setTenant(tenant);
        ratingEngine.setDisplayName(
                String.format(RatingEngine.DEFAULT_NAME_PATTERN, RatingEngine.DATE_FORMAT.format(new Date())));
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
            List<String> usedAttributesInSegment = findUsedAttributes(ratingEngine.getSegment());
            ruleBasedModel.setSelectedAttributes(usedAttributesInSegment);
            ratingEngine.addRatingModel(ruleBasedModel);
            if (ratingEngine.getStatus() == null) {
                ratingEngine.setStatus(RatingEngineStatus.INACTIVE);
            }
            ratingEngineDao.create(ratingEngine);
            break;
        case AI_BASED:
            break;
        default:
            break;
        }
    }

    @VisibleForTesting
    List<String> findUsedAttributes(MetadataSegment segment) {
        Set<String> usedAttributesSetInSegment = new HashSet<>();

        if (segment != null) {
            traverseAndRastriction(usedAttributesSetInSegment, segment.getAccountRestriction());
            traverseAndRastriction(usedAttributesSetInSegment, segment.getContactRestriction());
        }

        return new ArrayList<>(usedAttributesSetInSegment);
    }

    private void traverseAndRastriction(Set<String> usedAttributesInSegment, Restriction restriction) {
        if (restriction != null) {
            DepthFirstSearch search = new DepthFirstSearch();
            search.run(restriction, (object, ctx) -> {
                GraphNode node = (GraphNode) object;
                if (node instanceof ConcreteRestriction) {
                    ConcreteRestriction cr = (ConcreteRestriction) node;
                    Lookup lookup = cr.getLhs();
                    if (lookup instanceof AttributeLookup) {
                        usedAttributesInSegment.add(sanitize(((AttributeLookup) lookup).getAttribute()));
                    } else if (lookup instanceof SubQueryAttrLookup) {
                        usedAttributesInSegment.add(sanitize(((SubQueryAttrLookup) lookup).getAttribute()));
                    }
                } else if (node instanceof BucketRestriction) {
                    BucketRestriction bucket = (BucketRestriction) node;
                    usedAttributesInSegment.add(sanitize(bucket.getAttr().getAttribute()));
                }
            });
        }
    }

    private String sanitize(String attribute) {
        if (StringUtils.isNotBlank(attribute)) {
            attribute = attribute.trim();
            for (BusinessEntity entity : BusinessEntity.values()) {
                String prefix = entity.name() + ".";
                if (attribute.startsWith(prefix)) {
                    attribute = attribute.substring(prefix.length());
                }
            }
        }
        return attribute;
    }

}
