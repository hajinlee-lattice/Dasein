package com.latticeengines.datacloud.match.repository.reader;

import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.match.repository.ExternalColumnRepository;

@Transactional(readOnly = true, propagation = Propagation.REQUIRES_NEW)
public interface ExternalColumnReaderRepository extends ExternalColumnRepository {

}

