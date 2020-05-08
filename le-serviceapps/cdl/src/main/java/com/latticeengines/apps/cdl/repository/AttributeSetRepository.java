package com.latticeengines.apps.cdl.repository;

import java.util.List;

import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.AttributeSet;

public interface AttributeSetRepository extends BaseJpaRepository<AttributeSet, Long> {

    AttributeSet findByName(String name);

    AttributeSet findByDisplayName(String displayName);

    @Query("SELECT attrSet.name, attrSet.displayName, attrSet.description, attrSet.created, " +
            "attrSet.updated, attrSet.createdBy, attrSet.updatedBy from AttributeSet attrSet")
    List<AttributeSet> findAll();
}
