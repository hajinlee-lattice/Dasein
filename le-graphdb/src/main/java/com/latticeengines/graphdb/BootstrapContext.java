package com.latticeengines.graphdb;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

@Component
public class BootstrapContext {

    private ThreadLocal<Set<String>> thVertexExistenceSet;

    @PostConstruct
    public void initializeBean() {
        thVertexExistenceSet = new ThreadLocal<>();
    }

    public Set<String> initThreadLocalVertexExistenceSet() {

        Set<String> existingVertexExistenceSet = thVertexExistenceSet.get();
        if (existingVertexExistenceSet != null) {
            existingVertexExistenceSet.clear();
        }
        thVertexExistenceSet.set(new HashSet<>());
        return thVertexExistenceSet.get();
    }

    public void cleanupThreadLocalVertexExistenceSet() {
        Set<String> existingVertexExistenceSet = thVertexExistenceSet.get();
        if (existingVertexExistenceSet != null) {
            existingVertexExistenceSet.clear();
        }
        thVertexExistenceSet.set(null);
    }

    public boolean checkVertexExists(String vertexId) {
        Set<String> existingVertexExistenceSet = thVertexExistenceSet.get();
        if (existingVertexExistenceSet != null) {
            return existingVertexExistenceSet.contains(vertexId);
        }
        return false;
    }

    public void setVertexExists(String vertexId) {
        Set<String> existingVertexExistenceSet = thVertexExistenceSet.get();
        if (existingVertexExistenceSet != null) {
            existingVertexExistenceSet.add(vertexId);
        }
    }
}
