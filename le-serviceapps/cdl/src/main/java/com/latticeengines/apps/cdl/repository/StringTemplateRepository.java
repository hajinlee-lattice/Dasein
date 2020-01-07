package com.latticeengines.apps.cdl.repository;

import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.StringTemplate;

public interface StringTemplateRepository extends BaseJpaRepository<StringTemplate, Long> {
    StringTemplate findByPid(@Param("pid") Long pid);
    StringTemplate findByName(@Param("name") String name);
}
