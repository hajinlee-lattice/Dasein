package com.latticeengines.telepath.assemblers;

import static com.latticeengines.telepath.entities.Entity.__ENTITY_ID;
import static com.latticeengines.telepath.entities.Entity.__ENTITY_TYPE;
import static com.latticeengines.telepath.entities.Entity.__NAMESPACE;
import static com.latticeengines.telepath.entities.Entity.__TOMBSTONE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.loops;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.unfold;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.telepath.descriptors.BaseDescriptor;
import com.latticeengines.telepath.descriptors.Descriptor;
import com.latticeengines.telepath.descriptors.DescriptorRegistry;
import com.latticeengines.telepath.entities.Entity;
import com.latticeengines.telepath.entities.EntityRegistry;
import com.latticeengines.telepath.relations.BaseRelation;
import com.latticeengines.telepath.relations.Relation;
import com.latticeengines.telepath.relations.RelationRegistry;
import com.latticeengines.telepath.tools.ImpactKernel;
import com.latticeengines.telepath.tools.ValidationReport;

public abstract class BaseAssembler implements Assembler {

    private static final Logger log = LoggerFactory.getLogger(BaseAssembler.class);

    private Graph metaGraph;

    @PostConstruct
    public void registerEntitiesAndRelations() {
        getInvolvedEntities().forEach(entity -> EntityRegistry.register(entity.getEntityType(), entity.getClass()));
        getInvolvedRelations().forEach(relation -> RelationRegistry.register(relation.getType(), relation.getClass()));
    }

    protected Set<Entity> getSeedEntities(String namespace) {
        Graph metaGraph = assembleMetaGraph();
        List<Vertex> seedVertices = metaGraph.traversal().V().toList();
        Set<Entity> seedEntities = new HashSet<>();
        log.info("Using seed entity types for material graph: {}",
                seedVertices.stream().map(this::getVertexType).collect(Collectors.toList()));
        seedVertices.forEach(v -> {
            Class<? extends Entity> entityClass = EntityRegistry.getEntityClass(getVertexType(v));
            BaseDescriptor<? extends Entity> descriptor = DescriptorRegistry.getDescriptor(entityClass);
            seedEntities.addAll(descriptor.getAllEntities(namespace));
        });
        log.info("{} seed entitites: {}", seedEntities.size(), seedEntities.stream()
                .map(e -> String.format("%s - %s", e.getEntityType(), e.getEntityId())).collect(Collectors.toSet()));
        return seedEntities;
    }

    public Graph assembleMaterialGraph(String namespace) {
        return assembleMaterialGraph(getSeedEntities(namespace));
    }

    public Graph assembleMaterialGraph(Collection<Entity> seeds) {
        if (CollectionUtils.isEmpty(seeds)) {
            throw new IllegalStateException("No seed entities provided");
        }
        if (!seedsInSameNamespace(seeds)) {
            throw new IllegalArgumentException("Seeds are not in the same namespace.");
        }
        Graph materialGraph = TinkerGraph.open();
        GraphTraversalSource g = materialGraph.traversal();
        Queue<Entity> queue = new LinkedList<>(seeds);
        Set<String> visitedEntities = new HashSet<>();
        while (!queue.isEmpty()) {
            Entity root = queue.remove();
            String entityHash = root.getEntityHash();
            if (visitedEntities.contains(entityHash)) {
                continue;
            } else {
                visitedEntities.add(entityHash);
            }
            Vertex rootV = addObjectNode(g, root);
            Set<Entity> entitiesToBeVisited = new HashSet<>();
            for (Relation relation : getRelations(root)) {
                Entity relatedEntity = addRelationEntity(g, rootV, relation);
                if (relatedEntity != null) {
                    entitiesToBeVisited.add(relatedEntity);
                }
            }
            queue.addAll(entitiesToBeVisited);
        }
        return materialGraph;
    }

    private boolean seedsInSameNamespace(Collection<Entity> seeds) {
        Set<String> nameSpaces = seeds.stream()
                .map(entity -> entity.getProps().containsKey(__NAMESPACE)
                        ? entity.getProps().get(__NAMESPACE).toString()
                        : null)
                .filter(StringUtils::isNotBlank).collect(Collectors.toSet());
        log.info("name spaces: {}", nameSpaces);
        return nameSpaces.size() <= 1;
    }

    public ValidationReport validateIntegrity(Graph graph) {
        return validateIntegrity(graph, false);
    }

    public ValidationReport validateIntegrity(Graph graph, boolean checkTombstones) {
        GraphTraversalSource g = graph.traversal();
        ValidationReport report = new ValidationReport();
        Set<BaseRelation> unfulfilledRelations = new HashSet<>();
        g.E().forEachRemaining(edge -> {
            try {
                BaseRelation relation = buildRelationFromEdge(edge, checkTombstones);
                if (!relation.validate()) {
                    unfulfilledRelations.add(relation);
                }
            } catch (IllegalAccessException | InstantiationException e) {
                e.printStackTrace();
            }
        });
        List<Path> loops = g.V().as("a").repeat(__.out().simplePath()) //
                .emit(loops().is(P.gt(1))) //
                .out().where(P.eq("a")) //
                .path().dedup().by(unfold().order().by(T.id).dedup().fold()) //
                .toList();
        Set<List<Map<String, String>>> loopNodeProps = loops.stream().map(this::pathToNodeIdProps)
                .collect(Collectors.toSet());
        report.setUnfulfilledRelations(unfulfilledRelations);
        report.setLoops(loopNodeProps);
        if (checkTombstones) {
            Set<Map<String, String>> tombstones = new HashSet<>();
            g.V().has(__TOMBSTONE, true).forEachRemaining(v -> tombstones.add(getVertexIdProps(v)));
            report.setTombstones(tombstones);
        }
        return report;
    }

    @Override
    public Graph getUpdateKernel(Graph graph, Collection<Entity> entities) {
        Graph kernel = TinkerGraph.open();
        entities.forEach(entity -> {
            Descriptor<? extends Entity> descriptor = DescriptorRegistry.getDescriptor(entity.getClass());
            Graph subgraph = descriptor.getUpdateKernel(graph, entity);
            mergeSubgraphToKernel(subgraph, kernel);
        });
        return kernel;
    }

    private void mergeSubgraphToKernel(Graph fromGraph, Graph toGraph) {
        fromGraph.vertices().forEachRemaining(v -> {
            Entity kernelEntity = null;
            String entityType = getVertexType(v);
            try {
                kernelEntity = resolveDependencyEntity(entityType).newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            }
            assert kernelEntity != null;
            kernelEntity.extractPropsFromVertex(v);
            addObjectNode(toGraph.traversal(), kernelEntity);
        });
        fromGraph.edges().forEachRemaining(e -> {
            Vertex fromV = getNodeFromGraph(toGraph, getVertexIdProps(e.outVertex()));
            Vertex toV = getNodeFromGraph(toGraph, getVertexIdProps(e.inVertex()));
            addEdge(toGraph.traversal(), e.property(__EDGE_TYPE).value().toString(), fromV, toV);
        });
    }

    @Override
    public ImpactKernel getCreateKernel(Collection<Entity> entities) {
        ImpactKernel kernel = new ImpactKernel();
        entities.forEach(entity -> {
            Descriptor<? extends Entity> descriptor = DescriptorRegistry.getDescriptor(entity.getClass());
            ImpactKernel subKernel = descriptor.getCreateKernel(entity);
            kernel.entitiesToCreate.addAll(subKernel.entitiesToCreate);
            kernel.relationsToCreate.addAll(subKernel.relationsToCreate);
        });
        return kernel;
    }

    @Override
    public ImpactKernel getDeleteKernel(Collection<Entity> entities) {
        ImpactKernel kernel = new ImpactKernel();
        entities.forEach(entity -> {
            Descriptor<? extends Entity> descriptor = DescriptorRegistry.getDescriptor(entity.getClass());
            ImpactKernel subKernel = descriptor.getDeleteKernel(entity);
            kernel.entitiesToDelete.addAll(subKernel.entitiesToDelete);
            kernel.relationsToDelete.addAll(subKernel.relationsToDelete);
        });
        return kernel;
    }

    @Override
    public void commitUpdateToGraph(Graph graph, Collection<Entity> entities) {
        entities.forEach(entity -> {
            Vertex updateVertex = getNodeFromGraph(graph, entity);
            entity.updateVertex(updateVertex);
        });
    }

    @Override
    public void commitDeleteToGraph(Graph graph, Collection<Entity> entities) {
        entities.forEach(entity -> {
            Vertex deleteVertex = getNodeFromGraph(graph, entity);
            deleteVertex.remove();
        });
    }

    @Override
    public Graph assembleMetaGraph() {
        return assembleMetaGraph(getInvolvedEntities());
    }

    public abstract Collection<Entity> getInvolvedEntities();

    public abstract Set<BaseRelation> getInvolvedRelations();

    public Graph assembleMetaGraph(Collection<Entity> seeds) {
        if (CollectionUtils.isEmpty(seeds)) {
            throw new UnsupportedOperationException("No seed entities are provided.");
        }
        if (metaGraph == null) {
            Graph graph = TinkerGraph.open();
            GraphTraversalSource g = graph.traversal();

            Queue<Entity> queue = new LinkedList<>(seeds);
            Set<String> visitedEntities = new HashSet<>();
            do {
                Entity root = queue.remove();
                if (visitedEntities.contains(root.getEntityType())) {
                    continue;
                } else {
                    visitedEntities.add(root.getEntityType());
                }
                queue.addAll(root.getEntityRelations().stream().map(Pair::getLeft).collect(Collectors.toSet()));
                Vertex rootNode = addEntityNode(g, root.getEntityType());
                for (Pair<? extends Entity, ? extends BaseRelation> entityRelations : root.getEntityRelations()) {
                    Entity child = entityRelations.getLeft();
                    BaseRelation relation = entityRelations.getRight();
                    Vertex relatedNode = addEntityNode(g, child.getEntityType());
                    if (relation.isOutwardRelation()) {
                        addEdge(g, relation.getType(), rootNode, relatedNode);
                    } else {
                        addEdge(g, relation.getType(), relatedNode, rootNode);
                    }
                }
            } while (!queue.isEmpty());
            metaGraph = graph;
        }
        return metaGraph;
    }

    private Collection<Relation> getRelations(Entity root) {
        BaseDescriptor<? extends Entity> descriptor = DescriptorRegistry.getDescriptor(root.getClass());
        return descriptor.describeLocalEnvironment(root);
    }

    /**
     * Get the related entity. Default behavior is to fetch entity with entityId and
     * customerSpace defined by relation
     *
     * @param relation:
     *            describes a relation
     * @return related object with filled property bag, null if unable to fill
     *         entity
     */
    protected Entity getRelatedEntity(Relation relation) {
        Entity entityToFulfill = getEntityToFulfill(relation);
        String fullfilEntityId = getFulfillEntityId(relation);

        Entity relatedEntity;
        Map<String, Object> singleEntityProp = ImmutableMap.of( //
                Entity.__ENTITY_ID, fullfilEntityId, //
                Entity.__NAMESPACE, relation.getNameSpace() //
        );
        BaseDescriptor<? extends Entity> descriptor = DescriptorRegistry.getDescriptor(entityToFulfill.getClass());
        relatedEntity = descriptor.getDependencyEntity(singleEntityProp);
        return relatedEntity;
    }

    protected Entity addRelationEntity(GraphTraversalSource g, Vertex rootV, Relation relation) {
        Entity relatedEntity = getRelatedEntity(relation);
        Entity addedEntity = null;

        if (relatedEntity != null) {
            fillRelation(relation, relatedEntity);
            addedEntity = createAndLinkRelatedEntity(g, rootV, relatedEntity, relation);
        }
        return addedEntity;
    }

    private BaseRelation buildRelationFromEdge(Edge edge, boolean checkTombstones)
            throws IllegalAccessException, InstantiationException {
        BaseRelation relation = resolveRelation(edge.property(__EDGE_TYPE).value().toString()).newInstance();
        Vertex outV = edge.outVertex();
        String outVType = getVertexType(outV);
        Vertex inV = edge.inVertex();
        String inVType = getVertexType(inV);

        if (checkTombstones) {
            // mark tombstone
            markTombstone(outV);
            markTombstone(inV);
        }

        Entity fromEntity = resolveDependencyEntity(outVType).newInstance();
        fromEntity.extractPropsFromVertex(outV);
        Entity toEntity = resolveDependencyEntity(inVType).newInstance();
        toEntity.extractPropsFromVertex(inV);
        relation.setFromEntity(fromEntity);
        relation.setToEntity(toEntity);
        return relation;
    }

    protected void markTombstone(Vertex v) {
        String type = getVertexType(v);
        BaseDescriptor<? extends Entity> descriptor = DescriptorRegistry.getDescriptor(resolveDependencyEntity(type));
        if (descriptor.shouldMarkTombstone(v)) {
            v.property(__TOMBSTONE, true);
        }
    }

    private Entity createAndLinkRelatedEntity(GraphTraversalSource g, Vertex rootV, Entity entityToLink,
            Relation relation) {
        Vertex newV = addObjectNode(g, entityToLink);
        Vertex fromV = relation.isOutwardRelation() ? rootV : newV;
        Vertex toV = relation.isOutwardRelation() ? newV : rootV;
        addEdge(g, relation.getType(), fromV, toV);
        return entityToLink;
    }

    private Vertex addEntityNode(GraphTraversalSource g, String entityType) {
        if (g.V().has(entityType, __ENTITY_TYPE, entityType).hasNext()) {
            return g.V().has(entityType, __ENTITY_TYPE, entityType).next();
        } else {
            return g.addV(entityType).property(__ENTITY_TYPE, entityType).next();
        }
    }

    private Vertex addObjectNode(GraphTraversalSource g, Entity entity) {
        String pid = entity.getProps().get(__ENTITY_ID).toString();
        String entityType = entity.getEntityType();
        if (g.V().has(entityType, __ENTITY_TYPE, entityType).has(entityType, __ENTITY_ID, pid).hasNext()) {
            return g.V().has(entityType, __ENTITY_TYPE, entityType).has(entityType, __ENTITY_ID, pid).next();
        } else {
            return entity.createVertexAndAddToGraph(g);
        }
    }

    private Edge addEdge(GraphTraversalSource g, String edgeType, Vertex from, Vertex to) {
        if (g.V(from).outE(edgeType).where(__.inV().is(to)).has(edgeType, __EDGE_TYPE, edgeType).hasNext()) {
            return g.V(from).outE(edgeType).where(__.inV().is(to)).has(edgeType, __EDGE_TYPE, edgeType).next();
        } else {
            return g.addE(edgeType).from(from).to(to).property(__EDGE_TYPE, edgeType).next();
        }
    }

    protected void fillRelation(Relation relation, Entity relatedEntity) {
        if (relation.isOutwardRelation()) {
            relation.setToEntity(relatedEntity);
        } else {
            relation.setFromEntity(relatedEntity);
        }
    }

    protected Entity getEntityToFulfill(Relation relation) {
        return relation.isOutwardRelation() ? relation.getToEntity() : relation.getFromEntity();
    }

    protected String getFulfillEntityId(Relation relation) {
        return relation.isOutwardRelation() ? relation.getToEntityId() : relation.getFromEntityId();
    }

    protected Class<? extends BaseRelation> resolveRelation(String type) {
        return RelationRegistry.getRelationClass(type);
    }

    protected Class<? extends Entity> resolveDependencyEntity(String type) {
        return EntityRegistry.getEntityClass(type);
    }

    protected List<Map<String, String>> pathToNodeIdProps(Path path) {
        return path.stream().map(pair -> {
            Vertex vertex = (Vertex) pair.getValue0();
            return getVertexIdProps(vertex);
        }).collect(Collectors.toList());
    }

    public Map<String, String> getVertexIdProps(Vertex v) {
        String entityType = getVertexType(v);
        String entityId = getVertexId(v);
        String nameSpace = getVertexNameSpace(v);
        Map<String, String> props = new HashMap<>();
        props.put(__ENTITY_ID, entityId);
        props.put(__ENTITY_TYPE, entityType);
        if (StringUtils.isNotBlank(nameSpace)) {
            props.put(__NAMESPACE, nameSpace);
        }
        return props;
    }

    protected String getVertexType(Vertex v) {
        return v.property(__ENTITY_TYPE).value().toString();
    }

    protected String getVertexId(Vertex v) {
        return v.property(__ENTITY_ID).value().toString();
    }

    protected String getVertexNameSpace(Vertex v) {
        return v.property(__NAMESPACE).isPresent() ? v.property(__NAMESPACE).value().toString() : "";
    }

    public Vertex getNodeFromGraph(Graph graph, Entity entity) {
        String entityId = entity.getEntityId();
        String entityType = entity.getEntityType();
        return graph.traversal().V().has(__ENTITY_TYPE, entityType).has(__ENTITY_ID, entityId).next();
    }

    public Vertex getNodeFromGraph(Graph graph, Map<String, String> idProps) {
        String entityId = idProps.get(__ENTITY_ID);
        String entityType = idProps.get(__ENTITY_TYPE);
        return graph.traversal().V().has(__ENTITY_TYPE, entityType).has(__ENTITY_ID, entityId).next();
    }
}
