package com.latticeengines.domain.exposed.camille;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.google.common.base.Joiner;

/**
 * Represents an absolute path used by Camille. Valid paths must start with /
 * and contain word characters or .'s.
 */
public class Path implements Iterable<Path> {
    /**
     * Iterates through all the parent paths and this path. Uses a copy of this
     * object as it exists when this method is called.
     */
    @Override
    public Iterator<Path> iterator() {
        return new Iterator<Path>() {
            private final Iterator<Path> iter;

            {
                List<Path> paths = getParentPaths();
                paths.add(Path.this);
                iter = paths.iterator();
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public Path next() {
                return iter.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported");
            }
        };
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

    public int numParts() {
        return parts.size();
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
