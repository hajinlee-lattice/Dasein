package com.latticeengines.domain.exposed.swlib;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.traversal.impl.TopologicalTraverse;

public enum SoftwareLibrary {

    DataCloud("datacloud"), //
    LeadPrioritization("leadprioritization"), //
    ProspectDiscovery("prospectdiscovery"), //
    CDL("cdl");

    private static final Map<String, SoftwareLibrary> nameMap = new HashMap<>();

    private final String name;
    private Set<Module> modules;
    private final List<Dependency> depdencies = new ArrayList<>();

    static {
        for (SoftwareLibrary swlib: SoftwareLibrary.values()) {
            nameMap.put(swlib.getName(), swlib);
        }

        DataCloud.modules = Collections.singleton(Module.workflowapi);

        LeadPrioritization.modules = new HashSet<>(Arrays.asList(Module.workflowapi, Module.dataflowapi));
        LeadPrioritization.depdencies.add(new Dependency(Module.workflowapi, DataCloud));

        CDL.modules = new HashSet<>(Arrays.asList(Module.workflowapi, Module.dataflowapi));
        CDL.depdencies.add(new Dependency(Module.workflowapi, DataCloud));

        ProspectDiscovery.depdencies.add(new Dependency(Module.workflowapi, DataCloud));
        ProspectDiscovery.modules = new HashSet<>(Arrays.asList(Module.workflowapi, Module.dataflowapi));
    }

    public static SoftwareLibrary fromName(String name) {
        return nameMap.get(name);
    }

    public Set<Module> getModules() {
        return modules;
    }

    public static List<SoftwareLibrary> getLoadingSequence(Module module, List<SoftwareLibrary> libs) {
        TopologicalTraverse traverse = new TopologicalTraverse();
        List<Dependency> init = libs.stream().map(l -> new Dependency(module, l)).collect(Collectors.toList());
        List<Dependency> deps = traverse.sort(init, (dep) -> new Dependency(dep.module, dep.lib));
        List<SoftwareLibrary> depLibs = new ArrayList<>();
        // not use stream, because not sure how it handles ordering, which is important here.
        deps.forEach(d -> depLibs.add(d.lib));
        return depLibs;
    }

    SoftwareLibrary(String name) {
        this.name = name;
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
            return lib.depdencies.stream().filter(c -> module.equals(c.module)).collect(Collectors.toList());
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
