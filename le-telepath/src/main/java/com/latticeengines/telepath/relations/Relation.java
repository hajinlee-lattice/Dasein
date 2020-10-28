package com.latticeengines.telepath.relations;

import com.latticeengines.telepath.entities.Entity;

public interface Relation {
    String getType();

    String getNameSpace();

    void setNameSpace(String namespace);

    String getFromEntityId();

    String getToEntityId();

    Entity getFromEntity();

    void setFromEntity(Entity fromEntity);

    Entity getToEntity();

    void setToEntity(Entity fromEntity);

    boolean isOutwardRelation();

    boolean validate();
}
