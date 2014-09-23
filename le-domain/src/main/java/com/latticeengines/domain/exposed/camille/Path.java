package com.latticeengines.domain.exposed.camille;

import java.util.ArrayList;
import java.util.List;

public class Path {
    public Path(String rawPath) {
        parts = new ArrayList<String>();
        // TODO
        // Validate well-formed path (must start with a /, etc...)
    }
    
    public Path(List<String> parts) {
        // TODO Validation
        this.parts = parts;
    }

    public String getSuffix() {
        // TODO
        return null;
    }

    @Override
    public String toString() {
        // TODO
        return null;
    }
    
    public Path prefix(Path p) {
        // TODO
        return new Path("");
    }

    public Path append(Path p) {
        // TODO
        return new Path("");
    }
    
    private List<String> parts;
}
