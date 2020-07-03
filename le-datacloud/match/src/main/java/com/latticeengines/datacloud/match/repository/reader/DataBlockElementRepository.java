package com.latticeengines.datacloud.match.repository.reader;

import java.util.List;

import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockElement;

public interface DataBlockElementRepository extends BaseJpaRepository<DataBlockElement, Long> {

    @Query("SELECT block, level, primeColumn.primeColumnId FROM DataBlockElement")
    List<Object[]> getAllBlockElements();

}
