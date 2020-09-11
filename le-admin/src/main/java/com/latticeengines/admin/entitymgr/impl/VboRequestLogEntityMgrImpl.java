package com.latticeengines.admin.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.admin.entitymgr.VboRequestLogEntityMgr;
import com.latticeengines.admin.repository.reader.VboRequestLogReaderRepository;
import com.latticeengines.admin.repository.writer.VboRequestLogWriterRepository;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.dcp.vbo.VboCallback;
import com.latticeengines.domain.exposed.dcp.vbo.VboResponse;
import com.latticeengines.domain.exposed.vbo.VboRequestLog;

@Component("vboRequestLogEntityMgr")
public class VboRequestLogEntityMgrImpl extends JpaEntityMgrRepositoryImpl<VboRequestLog, Long> implements VboRequestLogEntityMgr {

    @Inject
    private VboRequestLogReaderRepository readerRepository;

    @Inject
    private VboRequestLogWriterRepository writerRepository;

    @Override
    public BaseJpaRepository<VboRequestLog, Long> getRepository() {
        return writerRepository;
    }

    @Override
    @Transactional(transactionManager = "vboJpaTransactionManager", propagation = Propagation.REQUIRED)
    public void save(VboRequestLog vboRequestLog) {
        writerRepository.save(vboRequestLog);
    }

    @Override
    @Transactional(transactionManager = "vboJpaTransactionManager", propagation = Propagation.REQUIRED)
    public void updateVboResponse(String traceId, VboResponse vboResponse) {
        VboRequestLog requestLog = writerRepository.findByTraceId(traceId);
        requestLog.setVboResponse(vboResponse);
        writerRepository.save(requestLog);
    }

    @Override
    @Transactional(transactionManager = "vboJpaTransactionManager", propagation = Propagation.REQUIRED)
    public void updateVboCallback(String traceId, VboCallback vboCallback, Long sendTime) {
        VboRequestLog requestLog = writerRepository.findByTraceId(traceId);
        requestLog.setCallbackRequest(vboCallback);
        requestLog.setCallbackTime(sendTime);
        writerRepository.save(requestLog);
    }

    @Override
    @Transactional(transactionManager = "vboJpaTransactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public VboRequestLog findByTraceId(String traceId) {
        return readerRepository.findByTraceId(traceId);
    }

    @Override
    @Transactional(transactionManager = "vboJpaTransactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<VboRequestLog> findByTenantId(String tenantId) {
        return readerRepository.findByTenantId(tenantId);
    }
}
