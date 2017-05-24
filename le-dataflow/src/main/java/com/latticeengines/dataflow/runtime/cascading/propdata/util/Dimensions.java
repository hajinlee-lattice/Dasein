package com.latticeengines.dataflow.runtime.cascading.propdata.util;

import java.util.List;

public class Dimensions {
    private List<Long> dimensions;

    private Dimensions() {
    }

    public Dimensions(List<Long> dimensions) {
        this();
        this.dimensions = dimensions;
    }

    public List<Long> getDimensions() {
        return dimensions;
    }

    public void setDimensions(List<Long> dimensions) {
        this.dimensions = dimensions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o != null && o instanceof Dimensions) {
            Dimensions secObj = (Dimensions) o;

            if ((secObj.dimensions == null && dimensions == null)) {
                return true;
            } else

            if (secObj.dimensions != null //
                    && dimensions != null //
                    && secObj.dimensions.size() == dimensions.size()) {
                int idx = 0;
                for (Long odim : dimensions) {
                    Long sdim = secObj.dimensions.get(idx++);
                    if (!odim.equals(sdim)) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dimensions == null) ? 0 : dimensions.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return dimensions == null ? "{}" : dimensions.toString();
    }
}
