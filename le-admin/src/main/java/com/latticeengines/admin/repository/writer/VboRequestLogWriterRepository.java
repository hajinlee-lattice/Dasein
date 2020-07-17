package com.latticeengines.admin.repository.writer;

import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.admin.repository.VboRequestLogRepository;

@Transactional
public interface VboRequestLogWriterRepository extends VboRequestLogRepository {
}
