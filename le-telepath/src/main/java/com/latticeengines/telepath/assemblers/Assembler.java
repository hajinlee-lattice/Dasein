package com.latticeengines.telepath.assemblers;

import java.util.Collection;

import org.apache.tinkerpop.gremlin.structure.Graph;

import com.latticeengines.telepath.entities.Entity;
import com.latticeengines.telepath.tools.ImpactKernel;
import com.latticeengines.telepath.tools.ValidationReport;

public interface Assembler {
    String __EDGE_TYPE = "__edgeType";

    /**
     * assembler material graph with all involved entities under the given namespace
     *
     * @param nameSpace
     * @return material graph
     */
    Graph assembleMaterialGraph(String nameSpace);

    /**
     * Given some seed entities, build a material graph
     *
     * @param seeds
     *            seed entities
     * @return material graph
     */
    Graph assembleMaterialGraph(Collection<Entity> seeds);

    /**
     * Validate all relations in a material graph without refresh tombstone status
     *
     * @param graph
     *            material graph
     * @return validation report
     */
    ValidationReport validateIntegrity(Graph graph);

    /**
     * Validate all relations in a material graph
     *
     * @param graph
     *            material graph
     * @param checkTombstones
     *            whether or not to check and mark tombstone vertices
     * @return validation report for full material graph
     */
    ValidationReport validateIntegrity(Graph graph, boolean checkTombstones);

    Graph getUpdateKernel(Graph graph, Collection<Entity> entities);

    ImpactKernel getCreateKernel(Collection<Entity> entities);

    /**
     * Given entities to delete, return impact kernel. Impact kernel contains: <br/>
     * entities to be deleted, including cascade delete defined by descriptor
     * relations to be removed defined by descriptor
     *
     * BaseDescriptor provides naive implementation that consider only entities and
     * their edges.
     *
     * @param entities
     *            entities to delete
     * @return impact kernel
     */
    ImpactKernel getDeleteKernel(Collection<Entity> entities);

    /**
     * commit property update to graph
     *
     * @param graph
     *            graph being updated
     * @param entities
     *            entities with properties after updating
     */
    void commitUpdateToGraph(Graph graph, Collection<Entity> entities);

    /**
     * commit delete to graph
     *
     * @param graph
     *            graph where vertices are deleted from
     * @param entities
     *            entities with properties after updating
     */
    void commitDeleteToGraph(Graph graph, Collection<Entity> entities);

    /**
     * Build dependency graph among dependency classes
     *
     * @param seeds:
     *            collection of seed classes
     * @return dependency graph among dependency classes
     */
    Graph assembleMetaGraph(Collection<Entity> seeds);

    /**
     * Build dependency graph among dependency classes
     *
     * @return dependency graph among dependency classes
     */
    Graph assembleMetaGraph();
}
