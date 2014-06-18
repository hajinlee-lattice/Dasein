package com.latticeengines.domain.exposed.dataplatform.dlorchestration;

import java.io.Serializable;

import com.latticeengines.domain.exposed.dataplatform.hibernate.GenericEnumUserType;

public class ModelCommandStatusUserType extends GenericEnumUserType<ModelCommandStatus> implements Serializable {
  
    private static final long serialVersionUID = 1L;

    public ModelCommandStatusUserType() {
        super(ModelCommandStatus.class);
    }
}
