package com.latticeengines.domain.exposed.camille;

import java.util.Arrays;
import java.util.List;

import com.google.common.base.Joiner;

/**
 * Represents an absolute path used by Camille.
 * Valid paths must start with / and contain word characters or .'s.
 */
public class Path {
    public Path(String rawPath) throws IllegalArgumentException {
        if (!rawPath.startsWith("/")) {
            throw new IllegalArgumentException("Path " + rawPath + " is invalid.  Paths must start with a /.");
        }
        
        if (!rawPath.matches("^(/[\\w.]+)+$")) {
            throw new IllegalArgumentException("Path " + rawPath + " is invalid.  A path must contain only word characters or .'s");
        }
        
        parts = Arrays.asList(rawPath.split("/"));
    }
    
    public Path(List<String> parts) throws IllegalArgumentException {
        for (String part : parts) {
            if (!part.matches("^\\w+$")) {
                throw new IllegalArgumentException("Provided path part " + part + " is invalid.");
            }
        }
        if (parts.size() == 0) {
            throw new IllegalArgumentException("Paths with length 0 are not allowed");
        }
        
        this.parts = parts;
    }

    public String getSuffix() {
        return this.parts.get(this.parts.size()-1);
    }

    @Override
    public String toString() {
        return Joiner.on("/").join(parts);
    }

    private List<String> parts;
}
