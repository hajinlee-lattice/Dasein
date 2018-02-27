package com.latticeengines.metadata.jpa;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.Attribute;


@Transactional(readOnly = true)
public interface AttributeRepository extends BaseJpaRepository<Attribute, Long> {

    long countByTable_Pid(Long tablePid);

    List<Attribute> findByTable_Pid(Long tablePid, Pageable pageable);

    List<Attribute> findByTable_Pid(Long tablePid);

}
