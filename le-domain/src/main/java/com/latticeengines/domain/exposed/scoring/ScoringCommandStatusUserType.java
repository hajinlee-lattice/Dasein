package com.latticeengines.domain.exposed.scoring;

import java.io.Serializable;

import com.latticeengines.domain.exposed.dataplatform.hibernate.GenericEnumUserType;

public class ScoringCommandStatusUserType extends GenericEnumUserType<ScoringCommandStatus>
        implements Serializable {

    private static final long serialVersionUID = 1L;

    public ScoringCommandStatusUserType() {
        super(ScoringCommandStatus.class);
    }
}
