package com.latticeengines.dataplatform.dao.impl;

import com.latticeengines.dataplatform.exposed.domain.HasId;

public class Sequence implements HasId<Long> {
    
    private Long id = null;
    
    Sequence(Long id) {
        this.id = id;
    }
    
    @Override
    public Long getId() {
        return id;
    }

    @Override
    public void setId(Long id) {
        this.id = id;
    }

}
