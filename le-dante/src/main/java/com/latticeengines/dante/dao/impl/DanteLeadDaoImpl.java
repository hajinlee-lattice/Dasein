package com.latticeengines.dante.dao.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dante.dao.DanteLeadDao;
import com.latticeengines.dantedb.exposed.dao.impl.BaseDanteDaoImpl;
import com.latticeengines.domain.exposed.dante.DanteLead;

@Component("danteLeadDao")
public class DanteLeadDaoImpl extends BaseDanteDaoImpl<DanteLead> implements DanteLeadDao {

    private static final Logger log = LoggerFactory.getLogger(DanteLeadDaoImpl.class);

    @Override
    protected Class<DanteLead> getEntityClass() {
        return DanteLead.class;
    }

}
