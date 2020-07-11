package com.latticeengines.datacloud.match.repository.reader;

import java.util.Collection;
import java.util.List;

import javax.persistence.QueryHint;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockElement;

@Transactional(readOnly = true, propagation = Propagation.REQUIRES_NEW)
public interface DataBlockElementRepository extends BaseJpaRepository<DataBlockElement, Long> {

    @Query("SELECT block, level, primeColumn.primeColumnId FROM DataBlockElement")
    List<Object[]> getAllBlockElements();

    @QueryHints(value = { @QueryHint(name = "javax.persistence.query.timeout", value = "90000") })
    List<DataBlockElement> findAllByPrimeColumn_PrimeColumnIdIn(Collection<String> elementIds);

}
