package com.latticeengines.telepath.relations;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.latticeengines.telepath.entities.Entity;

public abstract class BaseRelation implements Relation {

    @JsonIgnore
    private String nameSpace;

    @JsonIgnore
    private String fromEntityId;

    @JsonIgnore
    private String toEntityId;

    private Entity fromEntity;

    private Entity toEntity;

    @JsonIgnore
    public abstract boolean isOutwardRelation();

    /**
     * Define what condition(s) a child entity needs to meet to be created. This
     * will be invoked on top of base validations while validating a relation (edge)
     * in material graph
     *
     * @return true if validation succeed, false otherwise
     */
    protected boolean specifiedValidate() {
        return true;
    }

    public boolean validate() {
        Entity otherEntity = isOutwardRelation() ? toEntity : fromEntity;
        return concreteEntity(otherEntity) //
                && activeEntity(otherEntity) //
                && specifiedValidate();
    }

    private boolean concreteEntity(Entity entity) {
        return entity != null && entity.getProps().containsKey(Entity.__ENTITY_ID)
                && !entity.getProps().get(Entity.__ENTITY_ID).toString().equals(Entity.__ID_PLACEHOLDER);
    }

    private boolean activeEntity(Entity entity) {
        return entity != null && (!entity.getProps().containsKey(Entity.__TOMBSTONE)
                || !(boolean) entity.getProps().get(Entity.__TOMBSTONE));
    }

    public String getNameSpace() {
        return nameSpace;
    }

    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }

    @Override
    public String getFromEntityId() {
        return fromEntityId;
    }

    public void setFromEntityId(String fromEntityId) {
        this.fromEntityId = fromEntityId;
    }

    @Override
    public String getToEntityId() {
        return toEntityId;
    }

    public void setToEntityId(String toEntityId) {
        this.toEntityId = toEntityId;
    }

    @Override
    public Entity getFromEntity() {
        return fromEntity;
    }

    public void setFromEntity(Entity fromEntity) {
        this.fromEntity = fromEntity;
    }

    @Override
    public Entity getToEntity() {
        return toEntity;
    }

    public void setToEntity(Entity toEntity) {
        this.toEntity = toEntity;
    }
}
