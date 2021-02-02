package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.ListSegmentDao;
import com.latticeengines.apps.cdl.entitymgr.ListSegmentEntityMgr;
import com.latticeengines.apps.cdl.repository.ListSegmentRepository;
import com.latticeengines.apps.cdl.repository.reader.ListSegmentReaderRepository;
import com.latticeengines.apps.cdl.repository.writer.ListSegmentWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.ListSegment;

@Component("listSegmentEntityMgr")
public class ListSegmentEntityMgrImpl extends BaseReadWriteRepoEntityMgrImpl<ListSegmentRepository, ListSegment, Long> implements ListSegmentEntityMgr {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ListSegmentEntityMgrImpl.class);

    @Inject
    private ListSegmentEntityMgrImpl _self;

    @Inject
    private ListSegmentWriterRepository writerRepository;

    @Inject
    private ListSegmentReaderRepository readerRepository;

    @Inject
    private ListSegmentDao listSegmentDao;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public ListSegment updateListSegment(ListSegment incomingListSegment) {
        log.info("Updating list segment by external system {} and external segment {}.",
                incomingListSegment.getExternalSystem(), incomingListSegment.getExternalSegmentId());
        ListSegment existingListSegment = _self.findByExternalInfo(incomingListSegment.getExternalSystem(), incomingListSegment.getExternalSegmentId());
        if (existingListSegment != null) {
            cloneListSegmentForUpdate(existingListSegment, incomingListSegment);
            update(existingListSegment);
            return existingListSegment;
        } else {
            throw new RuntimeException("Segment does not already exists");
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ListSegment findByExternalInfo(String externalSystem, String externalSegmentId) {
        return writerRepository.findByExternalSystemAndExternalSegmentId(externalSystem, externalSegmentId);
    }

    private void cloneListSegmentForUpdate(ListSegment existingListSegment, ListSegment incomingListSegment) {
        if (incomingListSegment.getCsvAdaptor() != null) {
            existingListSegment.setCsvAdaptor(incomingListSegment.getCsvAdaptor());
        }
        if (MapUtils.isNotEmpty(incomingListSegment.getDataTemplates())) {
            Map<String, String> existingDataTemplates = existingListSegment.getDataTemplates();
            if (MapUtils.isEmpty(existingDataTemplates)) {
                existingDataTemplates = new HashMap<>();
                existingListSegment.setDataTemplates(existingDataTemplates);
            }
            for (Map.Entry<String, String> entry : incomingListSegment.getDataTemplates().entrySet()) {
                existingDataTemplates.put(entry.getKey(), entry.getValue());
            }
        }
        if (incomingListSegment.getConfig() != null) {
            existingListSegment.setConfig(incomingListSegment.getConfig());
        }
    }

    @Override
    protected ListSegmentRepository getReaderRepo() {
        return writerRepository;
    }

    @Override
    protected ListSegmentRepository getWriterRepo() {
        return readerRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<ListSegmentRepository, ListSegment, Long> getSelf() {
        return _self;
    }

    @Override
    public BaseDao<ListSegment> getDao() {
        return listSegmentDao;
    }
}
