package com.latticeengines.metadata.entitymgr.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.graph.utils.GraphUtils;
import com.latticeengines.common.exposed.util.HibernateUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentProperty;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.metadata.dao.SegmentDao;
import com.latticeengines.metadata.dao.SegmentPropertyDao;
import com.latticeengines.metadata.entitymgr.SegmentEntityMgr;
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
    private DataCollectionCache dataCollectionCache;

    @Override
    public BaseDao<MetadataSegment> getDao() {
        return segmentDao;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public MetadataSegment findByName(String name) {
        MetadataSegment segment = segmentDao.findByNameWithSegmentationDataCollection(name);
        initialize(segment);
        return segment;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public List<MetadataSegment> findAll() {
        return super.findAll().stream().map(this::initialize).collect(Collectors.toList());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public MetadataSegment findByName(String querySourceName, String name) {
        return segmentDao.findByDataCollectionAndName(querySourceName, name);
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
        MetadataSegment existing = findByName(segment.getName());
        if (existing != null) {
            delete(existing);
        }

        if (segment.getDataCollection() == null) {
            segment.setDataCollection(dataCollectionService.getDataCollectionByType(MultiTenantContext.getTenant()
                    .getId(), DataCollectionType.Segmentation));
        }

        // TODO temporary commented out for DEMO
        // addAttributeDependencies(segment);

        super.createOrUpdate(segment);
        for (MetadataSegmentProperty metadataSegmentProperty : segment.getMetadataSegmentProperties()) {
            metadataSegmentProperty.setMetadataSegment(segment);
            segmentPropertyDao.create(metadataSegmentProperty);
        }
    }

    private void addAttributeDependencies(MetadataSegment segment) {
        List<ColumnLookup> lookups = GraphUtils.getAllOfType(segment.getRestriction(), ColumnLookup.class);
        Set<ColumnLookup> set = new HashSet<>(lookups);
        DataCollection dataCollection = segment.getDataCollection();

        if (lookups.stream().anyMatch(l -> l.getObjectType() == null)) {
            throw new RuntimeException("All ColumnLookup object types must be specified");
        }
        List<Attribute> attributes = new ArrayList<>();
        for (ColumnLookup lookup : set) {
            Table table = dataCollection.getTable(lookup.getObjectType());
            if (table == null) {
                log.warn(String.format("No such Table in DataCollection %s with type %s", dataCollection.getName(),
                        lookup.getObjectType()));
                continue;
            }

            Attribute attribute = table.getAttribute(lookup.getColumnName());
            if (attribute == null) {
                log.warn(String.format("No such Attribute in Table %s with name %s", table.getName(),
                        lookup.getColumnName()));
                continue;
            }
            attributes.add(attribute);
        }

        segment.setAttributeDependencies(attributes);
    }

    private MetadataSegment initialize(MetadataSegment segment) {
        if (segment != null) {
            HibernateUtils.inflateDetails(segment.getAttributeDependencies());
        }
        return segment;
    }

}
