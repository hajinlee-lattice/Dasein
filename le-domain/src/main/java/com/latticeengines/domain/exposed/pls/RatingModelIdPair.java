package com.latticeengines.domain.exposed.pls;

import com.latticeengines.common.exposed.util.JsonUtils;

public class RatingModelIdPair {
    private String ratingEngineId;

    private String ratingModelId;

    public String getRatingEngineId() {
        return ratingEngineId;
    }

    public void setRatingEngineId(String ratingEngineId) {
        this.ratingEngineId = ratingEngineId;
    }

    public String getRatingModelId() {
        return ratingModelId;
    }

    public void setRatingModelId(String ratingModelId) {
        this.ratingModelId = ratingModelId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((ratingEngineId == null) ? 0 : ratingEngineId.hashCode());
        result = prime * result + ((ratingModelId == null) ? 0 : ratingModelId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RatingModelIdPair other = (RatingModelIdPair) obj;
        if (ratingEngineId == null) {
            if (other.ratingEngineId != null)
                return false;
        } else if (!ratingEngineId.equals(other.ratingEngineId))
            return false;
        if (ratingModelId == null) {
            if (other.ratingModelId != null)
                return false;
        } else if (!ratingModelId.equals(other.ratingModelId))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
