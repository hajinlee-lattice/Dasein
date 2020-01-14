package com.latticeengines.domain.exposed.swlib;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.traversal.impl.TopologicalTraverse;

public enum SoftwareLibrary {

    DataCloud("datacloud"), //
    Modeling("modeling"), //
    Scoring("scoring"), //
    LeadPrioritization("leadprioritization"), //
    CDL("cdl"),
    DCP("dcp");

    private static final Map<String, SoftwareLibrary> nameMap = new HashMap<>();

    static {
        for (SoftwareLibrary swlib : SoftwareLibrary.values()) {
            nameMap.put(swlib.getName(), swlib);
        }

        DataCloud.modules = Collections.singleton(Module.workflowapi);

        Modeling.modules = ImmutableSet.of(Module.workflowapi, Module.dataflowapi);
        Modeling.depdencies.add(new Dependency(Module.workflowapi, DataCloud));
        Modeling.depdencies.add(new Dependency(Module.workflowapi, Scoring));
        Modeling.depdencies.add(new Dependency(Module.dataflowapi, Scoring));

        Scoring.modules = ImmutableSet.of(Module.workflowapi, Module.dataflowapi);
        Scoring.depdencies.add(new Dependency(Module.workflowapi, DataCloud));

        LeadPrioritization.modules = ImmutableSet.of(Module.workflowapi, Module.dataflowapi);
        LeadPrioritization.depdencies.add(new Dependency(Module.workflowapi, DataCloud));

        CDL.modules = ImmutableSet.of(Module.workflowapi, Module.dataflowapi);
        DCP.modules = ImmutableSet.of(Module.workflowapi, Module.dataflowapi);
    }

    private final String name;
    private final List<Dependency> depdencies = new ArrayList<>();
    private Set<Module> modules;

    SoftwareLibrary(String name) {
        this.name = name;
    }

    public static SoftwareLibrary fromName(String name) {
        return nameMap.get(name);
    }

    public static List<SoftwareLibrary> getLoadingSequence(Module module,
            List<SoftwareLibrary> libs) {
        TopologicalTraverse traverse = new TopologicalTraverse();
        List<Dependency> init = libs.stream().map(l -> new Dependency(module, l))
                .collect(Collectors.toList());
        List<Dependency> deps = traverse.sort(init, dep -> new Dependency(dep.module, dep.lib));
        List<SoftwareLibrary> depLibs = new ArrayList<>();
        // not use stream, because not sure how it handles ordering, which is
        // important here.
        deps.forEach(d -> {
            if (!depLibs.contains(d.lib)) {
                depLibs.add(d.lib);
            }
        });
        return depLibs;
    }

    public Set<Module> getModules() {
        return modules;
    }

    public String getName() {
        return name;
    }

    public List<SoftwareLibrary> getLoadingSequence(Module module) {
        return getLoadingSequence(module, Collections.singletonList(this));
    }

    public enum Module {
        workflowapi, dataflowapi
    }

    private static class Dependency implements GraphNode {
        private final SoftwareLibrary lib;
        private final Module module;

        private Dependency(Module module, SoftwareLibrary lib) {
            this.lib = lib;
            this.module = module;
        }

        @Override
        public Collection<? extends GraphNode> getChildren() {
            return lib.depdencies.stream().filter(c -> module.equals(c.module))
                    .collect(Collectors.toList());
        }

        @Override
        public Map<String, Collection<? extends GraphNode>> getChildMap() {
            return Collections.emptyMap();
        }

        @Override
        public int hashCode() {
            return HashCodeBuilder.reflectionHashCode(this);
        }

        @Override
        public boolean equals(Object obj) {
            return EqualsBuilder.reflectionEquals(this, obj);
        }

    }
}
