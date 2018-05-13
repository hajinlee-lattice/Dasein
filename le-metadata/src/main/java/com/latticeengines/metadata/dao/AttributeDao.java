package com.latticeengines.metadata.dao;

import java.util.List;

import org.springframework.data.domain.Pageable;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.Attribute;

public interface AttributeDao extends BaseDao<Attribute> {

    Long countByTablePid(Long tablePid);

    List<Attribute> findByTablePid(Long tablePid, Pageable pageable);
}
