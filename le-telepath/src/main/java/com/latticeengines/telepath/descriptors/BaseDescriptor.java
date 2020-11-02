package com.latticeengines.telepath.descriptors;

import static com.latticeengines.telepath.entities.BaseEntity.__ENTITY_ID;
import static com.latticeengines.telepath.entities.BaseEntity.__ENTITY_TYPE;
import static com.latticeengines.telepath.entities.Entity.__NAMESPACE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.telepath.entities.BaseEntity;
import com.latticeengines.telepath.entities.Entity;
import com.latticeengines.telepath.relations.BaseRelation;
import com.latticeengines.telepath.relations.Relation;
import com.latticeengines.telepath.tools.ImpactKernel;

public abstract class BaseDescriptor<T extends Entity> implements Descriptor<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseDescriptor.class);

    @PostConstruct
    public void postConstruct() {
        DescriptorRegistry.register(getDependencyEntityClass(), this);
    }

    protected abstract Class<T> getDependencyEntityClass();

    /**
     * use props to fetch single object from database
     *
     * @param props:
     *            properties used for query
     * @return object specified by props
     */
    public abstract Entity getDependencyEntity(Map<String, Object> props);

    public boolean shouldMarkTombstone(Vertex v) {
        Entity entity = null;
        try {
            entity = getDependencyEntityClass().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
        assert entity != null;
        entity.extractPropsFromVertex(v);
        return getDependencyEntity(entity.getProps()) == null;
    }

    /**
     * use props to fetch objects from database
     *
     * @return objects specified by props
     */
    public List<Entity> getAllEntities(String namespace) {
        return Collections.emptyList();
    }

    /**
     * get all relations from an entity that need to be fulfilled
     *
     * @param entity:
     *            root entity
     *
     * @return all relations from root entity
     */
    public abstract List<Relation> describeLocalEnvironment(Entity entity);

    public Graph getUpdateKernel(Graph graph, Entity entity) {
        GraphTraversalSource g = graph.traversal();
        String entityId = entity.getEntityId();
        String entityType = entity.getEntityType();
        Vertex entityVertex = graph.traversal().V().has(__ENTITY_TYPE, entityType).has(__ENTITY_ID, entityId).next();
        return (Graph) g.V(entityVertex).bothE().subgraph("subGraph").cap("subGraph").next();
    }

    @Override
    public ImpactKernel getCreateKernel(Entity entity) {
        ImpactKernel kernel = new ImpactKernel();
        kernel.entitiesToCreate.add(entity);
        kernel.relationsToCreate.addAll(describeLocalEnvironment(entity));
        return kernel;
    }

    public ImpactKernel getDeleteKernel(Entity entity) {
        ImpactKernel kernel = new ImpactKernel();
        kernel.entitiesToDelete.add(entity);
        kernel.relationsToDelete.addAll(describeLocalEnvironment(entity));
        return kernel;
    }

    public String commitCreate(Object obj) {
        throw new IllegalStateException("Create commit not implemented");
    }

    public void commitUpdate(Entity entity) {
        throw new IllegalStateException("Update commit not implemented");
    }

    public void commitDelete(Entity entity) {
        throw new IllegalStateException("Delete commit not implemented");
    }

    @Override
    public T toEntity(String namespace, String entityId) {
        T entity = null;
        try {
            entity = getDependencyEntityClass().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
        assert entity != null;
        entity.getProps().put(__ENTITY_ID, entityId);
        entity.getProps().put(__NAMESPACE, namespace);
        return entity;
    }

    @Override
    public List<Relation> toRelation(@NotNull String namespace, @NotNull String entityId, String tgtEntityType,
            @NotNull String tgtEntityId) {
        T entity = toEntity(namespace, entityId);
        List<Pair<? extends BaseEntity, ? extends BaseRelation>> relationPairs = entity.getEntityRelations();
        List<Relation> relations = new ArrayList<>();
        relationPairs.forEach(pair -> {
            Entity relatedEntity = pair.getKey();
            Relation relation = pair.getValue();
            if (relatedEntity.getEntityType().equals(tgtEntityType)) {
                relations.add(constructSimpleRelation(entity, relatedEntity, relation, tgtEntityId, namespace));
            }
        });
        return relations;
    }

    private Relation constructSimpleRelation(T entity, Entity relatedEntity, Relation relation, String tgtEntityId, String namespace) {
        boolean isOutward = relation.isOutwardRelation();
        Entity clonedRelated = null;
        Relation clonedRelation = null;
        try {
            clonedRelated = relatedEntity.getClass().newInstance();
            clonedRelation = relation.getClass().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
        assert clonedRelated != null && clonedRelation != null;
        clonedRelated.getProps().put(__ENTITY_ID, tgtEntityId);
        clonedRelated.getProps().put(__NAMESPACE, namespace);
        clonedRelation.setFromEntity(isOutward ? entity : clonedRelated);
        clonedRelation.setToEntity(isOutward ? clonedRelated : entity);

        return clonedRelation;
    }
}
