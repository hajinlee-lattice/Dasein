package com.latticeengines.dataplatform.dao.impl;

import java.util.Collections;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.ModelCommandDao;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;

@Component("modelCommandDao")
public class ModelCommandDaoImpl extends BaseDaoImpl<ModelCommand> implements ModelCommandDao {

    public ModelCommandDaoImpl() {
        super();
    }

    @Override
    public ModelCommand deserialize(String id, String content) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<ModelCommand> getNewAndInProgress() {
        // TODO select where CommandStatus = 0 OR CommandStatus = 1
        return Collections.emptyList();
    }

}