package com.latticeengines.telepath.descriptors;

import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Graph;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.telepath.entities.Entity;
import com.latticeengines.telepath.relations.Relation;
import com.latticeengines.telepath.tools.ImpactKernel;

public interface Descriptor<T extends Entity> {

    // mainly used by assembler to gather seed objects of an entity type
    List<Entity> getAllEntities(String namespace);

    Graph getUpdateKernel(Graph graph, Entity entity);

    ImpactKernel getCreateKernel(Entity entity);

    ImpactKernel getDeleteKernel(Entity entity);

    String commitCreate(Object obj);

    void commitUpdate(Entity entity); // only support property update

    void commitDelete(Entity entity);

    T toEntity(@NotNull String namespace, @NotNull String entityId);

    List<Relation> toRelation(@NotNull String namespace, @NotNull String entityId,
            @NotNull String tgtEntityType, @NotNull String tgtEntityId);
}
