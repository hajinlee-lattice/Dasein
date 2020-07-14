package com.latticeengines.datacloud.match.repository.reader;

import java.util.Collection;
import java.util.List;

import javax.persistence.QueryHint;

import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn;

@Transactional(readOnly = true, propagation = Propagation.REQUIRES_NEW)
public interface PrimeColumnRepository extends BaseJpaRepository<PrimeColumn, Long> {

    @QueryHints(value = { @QueryHint(name = "javax.persistence.query.timeout", value = "90000") })
    List<PrimeColumn> findAllByPrimeColumnIdIn(Collection<String> elementIds);

}
