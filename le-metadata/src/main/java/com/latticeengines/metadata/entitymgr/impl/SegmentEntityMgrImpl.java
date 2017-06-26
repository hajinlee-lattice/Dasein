package com.latticeengines.metadata.entitymgr.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.graph.utils.GraphUtils;
import com.latticeengines.common.exposed.util.HibernateUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentProperty;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentPropertyName;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.metadata.dao.SegmentDao;
import com.latticeengines.metadata.dao.SegmentPropertyDao;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.SegmentEntityMgr;
import com.latticeengines.metadata.entitymgr.StatisticsContainerEntityMgr;
import com.latticeengines.metadata.service.DataCollectionService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("segmentEntityMgr")
public class SegmentEntityMgrImpl extends BaseEntityMgrImpl<MetadataSegment> implements SegmentEntityMgr {
    private static final Log log = LogFactory.getLog(SegmentEntityMgrImpl.class);

    @Autowired
    private SegmentDao segmentDao;

    @Autowired
    private DataCollectionService dataCollectionService;

    @Autowired
    private SegmentPropertyDao segmentPropertyDao;

    @Autowired
    private StatisticsContainerEntityMgr statisticsContainerEntityMgr;

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Override
    public BaseDao<MetadataSegment> getDao() {
        return segmentDao;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public MetadataSegment findByName(String name) {
        MetadataSegment segment = segmentDao.findByField("name", name);
        inflate(segment);
        return segment;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public List<MetadataSegment> findAll() {
        return super.findAll().stream().map(this::inflate)
                .filter(segment -> !Boolean.TRUE.equals(segment.getMasterSegment()))
                .collect(Collectors.toList());
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void delete(MetadataSegment segment) {
        segment.getAttributeDependencies().clear();
        segmentDao.update(segment);
        segmentDao.delete(segment);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void createOrUpdate(MetadataSegment segment) {
        segment.setTenant(MultiTenantContext.getTenant());
        if (segment.getDataCollection() == null) {
            String defaultCollectionName = dataCollectionEntityMgr.getDefaultCollectionName();
            DataCollection defaultCollection = dataCollectionEntityMgr.getDataCollection(defaultCollectionName);
            segment.setDataCollection(defaultCollection);
        }
        if (Boolean.TRUE.equals(segment.getMasterSegment())) {
            MetadataSegment master = findMasterSegment(segment.getDataCollection().getName());
            if (master != null && !master.getName().equals(segment.getName())) {
                // master exists and not the incoming one
                segment.setMasterSegment(false);
            }
        }
        if (StringUtils.isBlank(segment.getName())) {
            segment.setName(NamingUtils.timestamp("Segment"));
        }

        MetadataSegment existing = findByName(segment.getName());
        if (existing != null) {
            segment = cloneForUpdate(existing, segment);
            HibernateUtils.inflateDetails(segment.getProperties());
            segment.getProperties().forEach(p -> segmentPropertyDao.delete(p));
            segmentDao.update(segment);
        } else {
            segmentDao.create(segment);
        }
        for (MetadataSegmentProperty metadataSegmentProperty : segment.getProperties()) {
            metadataSegmentProperty.setOwner(segment);
            segmentPropertyDao.create(metadataSegmentProperty);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public List<MetadataSegment> findAllInCollection(String collectionName) {
        return super.findAll().stream() //
                .filter(s -> s.getDataCollection().getName().equals(collectionName)) //
                .filter(segment -> !Boolean.TRUE.equals(segment.getMasterSegment())) //
                .map(this::inflate).collect(Collectors.toList());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public MetadataSegment findMasterSegment(String collectionName) {
        return segmentDao.findAll().stream() //
                .filter(s -> s.getDataCollection().getName().equals(collectionName)) //
                .filter(s -> Boolean.TRUE.equals(s.getMasterSegment())) //
                .map(this::inflate).findFirst().orElse(null);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void upsertStats(String segmentName, StatisticsContainer statisticsContainer) {
        MetadataSegment segment = findByName(segmentName);
        if (segment == null) {
            throw new IllegalStateException("Cannot find segment named " + segmentName + " in database");
        }
        StatisticsContainer oldStats = statisticsContainerEntityMgr.findInSegment(segmentName);
        if (oldStats != null) {
            log.info("There is already a main stats for segment " + segmentName + ". Remove it first.");
            statisticsContainerEntityMgr.delete(oldStats);
        }
        if (StringUtils.isBlank(statisticsContainer.getName())) {
            statisticsContainer.setName(NamingUtils.timestamp("Stats"));
        }
        statisticsContainer.setSegment(segment);
        statisticsContainer.setTenant(MultiTenantContext.getTenant());
        statisticsContainerEntityMgr.create(statisticsContainer);
        Statistics statistics = statisticsContainer.getStatistics();
        if (statistics != null) {
            updateCountsByStats(statistics, segment);
        }
    }

    private void updateCountsByStats(Statistics statistics, MetadataSegment segment) {
        Map<BusinessEntity, Long> counts = statistics.getCounts();
        if (counts != null && !counts.isEmpty()) {
            boolean propUpdated = false;
            if (counts.containsKey(BusinessEntity.Account)) {
                MetadataSegmentProperty prop = new MetadataSegmentProperty();
                prop.setOption(MetadataSegmentPropertyName.NumAccounts.getName());
                prop.setValue(String.valueOf(counts.get(BusinessEntity.Account)));
                segment.addSegmentProperty(prop);
                propUpdated = true;
            }

            if (counts.containsKey(BusinessEntity.Contact)) {
                MetadataSegmentProperty prop = new MetadataSegmentProperty();
                prop.setOption(MetadataSegmentPropertyName.NumContacts.getName());
                prop.setValue(String.valueOf(counts.get(BusinessEntity.Contact)));
                segment.addSegmentProperty(prop);
                propUpdated = true;
            }

            if (propUpdated) {
                for (MetadataSegmentProperty metadataSegmentProperty : segment.getProperties()) {
                    metadataSegmentProperty.setOwner(segment);
                    segmentPropertyDao.createOrUpdate(metadataSegmentProperty);
                }
            }
        }
    }

    private void addAttributeDependencies(MetadataSegment segment) {
        List<AttributeLookup> lookups = GraphUtils.getAllOfType(segment.getRestriction(), AttributeLookup.class);
        Set<AttributeLookup> set = new HashSet<>(lookups);
        DataCollection dataCollection = segment.getDataCollection();
        List<Attribute> attributes = new ArrayList<>();
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        String collectionName = segment.getDataCollection().getName();
        for (AttributeLookup lookup : set) {
            TableRoleInCollection tableRole = lookup.getEntity().getServingStore();
            List<Table> tables = dataCollectionService.getTables(customerSpace.toString(), collectionName, tableRole);
            if (tables == null || tables.isEmpty()) {
                log.warn(String.format("No serving Table in DataCollection %s for entity %s", dataCollection.getName(),
                        lookup.getEntity()));
                continue;
            }
            Table table = tables.get(0);
            Attribute attribute = table.getAttribute(lookup.getAttribute());
            if (attribute == null) {
                log.warn(String.format("No such Attribute in Table %s with name %s", table.getName(),
                        lookup.getAttribute()));
                continue;
            }
            attributes.add(attribute);
        }

        segment.setAttributeDependencies(attributes);
    }

    private MetadataSegment inflate(MetadataSegment segment) {
        if (segment != null) {
            addAttributeDependencies(segment);
        }
        return segment;
    }

    private MetadataSegment cloneForUpdate(MetadataSegment existing, MetadataSegment incoming) {
        existing.setRestriction(incoming.getRestriction());
        existing.setMasterSegment(incoming.getMasterSegment());
        existing.setDisplayName(incoming.getDisplayName());
        existing.setDescription(incoming.getDescription());
        existing.setSegmentPropertyBag(incoming.getSegmentPropertyBag());
        existing.setMasterSegment(incoming.getMasterSegment());
        existing.setUpdated(new Date());
        return existing;
    }

}
