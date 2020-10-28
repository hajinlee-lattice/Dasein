package com.latticeengines.telepath;

import static com.latticeengines.telepath.entities.Entity.__NAMESPACE;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.telepath.testartifacts.assemblers.TestAssembler;
import com.latticeengines.telepath.testframework.GraphFunctionalTestNGBase;
import com.latticeengines.telepath.tools.ValidationReport;

public class AssemblerTestNG extends GraphFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AssemblerTestNG.class);

    private static Graph metaGraph;
    private static Graph materialGraph0;
    private static Graph materialGraph1;

    @Inject
    private TestAssembler testAssembler;

    @Test(groups = "functional")
    public void testMetaGraph() {
        metaGraph = testAssembler.assembleMetaGraph();
        log.info("meta graph: {}", metaGraph);
        Assert.assertEquals(Long.valueOf(testAssembler.getInvolvedEntities().size()),
                metaGraph.traversal().V().count().next());
    }

    @Test(groups = "functional", dependsOnMethods = "testMetaGraph")
    public void testMaterialGraph() {
        materialGraph0 = testAssembler.assembleMaterialGraph(TestAssembler.NAMESPACE_0);
        materialGraph1 = testAssembler.assembleMaterialGraph(TestAssembler.NAMESPACE_1);
        log.info("material graph with namespace {}: {}", TestAssembler.NAMESPACE_0, materialGraph0);
        log.info("material graph with namespace {}: {}", TestAssembler.NAMESPACE_1, materialGraph1);
        Assert.assertEquals(materialGraph0.traversal().V().count().next(),
                materialGraph1.traversal().V().count().next());
        ValidationReport m0Report = testAssembler.validateIntegrity(materialGraph0);
        ValidationReport m1Report = testAssembler.validateIntegrity(materialGraph1);
        Assert.assertTrue(m0Report.isSuccess());
        Assert.assertTrue(m1Report.isSuccess());
        Assert.assertTrue(verticesInSameNamespace(materialGraph0));
        Assert.assertTrue(verticesInSameNamespace(materialGraph1));
    }

    private boolean verticesInSameNamespace(Graph graph) {
        Set<String> nameSpaces = new HashSet<>();
        graph.traversal().V().forEachRemaining(v -> {
            String namespace = v.property(__NAMESPACE).isPresent() ? v.property(__NAMESPACE).value().toString() : "";
            nameSpaces.add(namespace);
        });
        return nameSpaces.stream().filter(StringUtils::isNotBlank).collect(Collectors.toSet()).size() <= 1;
    }
}
