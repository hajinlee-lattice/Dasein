package com.latticeengines.domain.exposed.camille;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

import com.google.common.base.Joiner;

/**
 * Represents an absolute path used by Camille. Valid paths must start with /
 * and contain word characters or .'s.
 */
public class Path implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<String> parts;

    public Path(String rawPath) throws IllegalArgumentException {
        if (!rawPath.startsWith("/")) {
            throw new IllegalArgumentException("Path " + rawPath + " is invalid.  Paths must start with a /.");
        }

        if (!rawPath.equals("/") && !rawPath.matches("^(/[\\w\\-.]+)+$")) {
            throw new IllegalArgumentException("Path " + rawPath
                    + " is invalid.  A path must contain only word characters or .'s");
        }

        if (rawPath.equals("/")) {
            parts = new ArrayList<String>();
        } else {
            String path = rawPath.substring(1, rawPath.length());
            parts = Arrays.asList(path.split("/"));
        }
    }

    public Path(List<String> parts) throws IllegalArgumentException {
        for (String part : parts) {
            if (!part.matches("^[\\w\\-.]+$")) {
                throw new IllegalArgumentException("Provided path array part " + part + " is invalid.");
            }
        }

        this.parts = parts;
    }

    public Path(String... parts) throws IllegalArgumentException {
        for (String part : parts) {
            if (!part.matches("^[\\w\\-.]+$")) {
                throw new IllegalArgumentException("Provided path array part " + part + " is invalid.");
            }
        }

        this.parts = Arrays.asList(parts);
    }

    public int numParts() {
        return parts.size();
    }

    public List<String> getParts() {
        return Collections.unmodifiableList(parts);
    }

    public Path append(String part) {
        if (part.contains("/")) {
            return append(new Path(part));
        }
        List<String> concat = new ArrayList<String>();
        concat.addAll(this.parts);
        concat.add(part);
        return new Path(concat);
    }

    public Path prefix(String part) {
        if (part.contains("/")) {
            return prefix(new Path(part));
        }
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

    public boolean startsWith(Path prefix) {
        if (prefix.isRoot()) {
            return true;
        }

        if (prefix.parts.size() > parts.size()) {
            return false;
        }

        for (int i = 0; i < prefix.parts.size(); ++i) {
            String part = parts.get(i);
            String prefixPart = prefix.parts.get(i);
            if (!part.equals(prefixPart)) {
                return false;
            }
        }

        return true;
    }

    public boolean isRoot() {
        return parts.size() == 0;
    }

    /**
     * Returns local path relative to the specified prefix. So for example, if
     * the path is /a/b/c/d, and the prefix is /a/b, this will return the path
     * /c/d.
     * 
     * @throws IllegalArgumentException
     *             if the path does not start with the specified prefix.
     */
    public Path local(Path prefix) {
        if (prefix.isRoot()) {
            return new Path(parts);
        }

        if (!startsWith(prefix)) {
            throw new IllegalArgumentException("Path " + toString() + " does not start with the prefix " + prefix);
        }

        return new Path(toString().substring(prefix.toString().length()));
    }

    public Path parent() {
        if (isRoot()) {
            throw new IllegalArgumentException("Cannot return the parent of root path " + this);
        }
        List<String> parentParts = new ArrayList<String>();
        parentParts.addAll(parts.subList(0, parts.size() - 1));
        return new Path(parentParts);
    }

    /**
     * @return The parent paths in order.Uses a copy of this object as it exists
     *         when this method is called.
     */
    public List<Path> getParentPaths() {
        ListIterator<String> iter = parts.listIterator();

        int size = parts.size();

        List<String> pathStrings = new ArrayList<String>(size);
        pathStrings.add(iter.next());

        List<Path> out = new ArrayList<Path>(size);
        out.add(new Path(new ArrayList<String>(pathStrings)));

        while (iter.nextIndex() < size - 1) {
            pathStrings.add(iter.next());
            out.add(new Path(new ArrayList<String>(pathStrings)));
        }

        return out;
    }

    public String getSuffix() {
        if (isRoot()) {
            return "";
        }
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
}
