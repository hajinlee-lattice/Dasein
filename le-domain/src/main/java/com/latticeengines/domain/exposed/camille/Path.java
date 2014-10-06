package com.latticeengines.domain.exposed.camille;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Joiner;

/**
 * Represents an absolute path used by Camille. Valid paths must start with /
 * and contain word characters or .'s.
 */
public class Path {
    public Path(String rawPath) throws IllegalArgumentException {
        if (!rawPath.startsWith("/")) {
            throw new IllegalArgumentException("Path " + rawPath + " is invalid.  Paths must start with a /.");
        }

        if (!rawPath.matches("^(/[\\w\\-.]+)+$")) {
            throw new IllegalArgumentException("Path " + rawPath
                    + " is invalid.  A path must contain only word characters or .'s");
        }

        String path = rawPath.substring(1, rawPath.length());

        parts = Arrays.asList(path.split("/"));
    }

    public Path(List<String> parts) throws IllegalArgumentException {
        for (String part : parts) {
            if (!part.matches("^[\\w\\-.]+$")) {
                throw new IllegalArgumentException("Provided path array part " + part + " is invalid.");
            }
        }
        if (parts.size() == 0) {
            throw new IllegalArgumentException("Path arrays with length 0 are not allowed");
        }

        this.parts = parts;
    }

    public Path(String... parts) throws IllegalArgumentException {
        for (String part : parts) {
            if (!part.matches("^[\\w\\-.]+$")) {
                throw new IllegalArgumentException("Provided path array part " + part + " is invalid.");
            }
        }
        if (parts.length == 0) {
            throw new IllegalArgumentException("Path arrays with length 0 are not allowed");
        }

        this.parts = Arrays.asList(parts);
    }

    public Path append(String part) {
        List<String> concat = new ArrayList<String>();
        concat.addAll(this.parts);
        concat.add(part);
        return new Path(concat);
    }

    public Path prefix(String part) {
        List<String> concat = new ArrayList<String>();
        concat.add(part);
        concat.addAll(this.parts);
        return new Path(concat);
    }
    
    public Path append(Path path) {
        List<String> concat = new ArrayList<String>();
        concat.addAll(parts);
        concat.addAll(path.parts);
        return new Path(concat);
    }
    
    public Path prefix(Path path) {
        List<String> concat = new ArrayList<String>();
        concat.addAll(path.parts);
        concat.addAll(parts);
        return new Path(concat);
    }

    public String getSuffix() {
        return this.parts.get(this.parts.size() - 1);
    }

    @Override
    public String toString() {
        return "/" + Joiner.on("/").join(parts);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other == null)
            return false;
        if (getClass() != other.getClass())
            return false;
        
        Path otherPath = (Path) other;

        if (otherPath.parts.size() != parts.size())
            return false;

        for (int i = 0; i < parts.size(); ++i) {
            String part = parts.get(i);
            String otherPart = otherPath.parts.get(i);
            if (!part.equals(otherPart)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((parts == null) ? 0 : parts.hashCode());
        return result;
    }

    private List<String> parts;
}
