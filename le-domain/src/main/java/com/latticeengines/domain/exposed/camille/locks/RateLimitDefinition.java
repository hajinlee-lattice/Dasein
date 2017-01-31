package com.latticeengines.domain.exposed.camille.locks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RateLimitDefinition {

    private boolean crossDivision = false;
    private Map<String, List<Quota>> quotas;

    public static RateLimitDefinition crossDivisionDefinition() {
        RateLimitDefinition definition = new RateLimitDefinition();
        definition.setCrossDivision(true);
        return definition;
    }

    public static RateLimitDefinition divisionPrivateDefinition() {
        RateLimitDefinition definition = new RateLimitDefinition();
        definition.setCrossDivision(false);
        return definition;
    }

    private RateLimitDefinition() {}

    public boolean isCrossDivision() {
        return crossDivision;
    }

    private void setCrossDivision(boolean crossDivision) {
        this.crossDivision = crossDivision;
    }

    public Map<String, List<Quota>> getQuotas() {
        return quotas;
    }

    private void setQuotas(Map<String, List<Quota>> quotas) {
        this.quotas = new HashMap<>();
        if (quotas != null) {
            quotas.entrySet().stream().filter(entry -> entry.getValue() != null).forEach(entry -> {
                entry.getValue().forEach(quota -> addQuota(entry.getKey(), quota));
            });
        }
    }

    public void addQuota(String counter, Quota quota) {
        if (quotas == null) {
            quotas = new HashMap<>();
        }
        if (!quotas.containsKey(counter)) {
            quotas.putIfAbsent(counter, new ArrayList<>());
        }
        if (!quotas.get(counter).contains(quota)) {
            quotas.get(counter).add(quota);
        }
    }

    public static class Quota {

        private final int maxQuantity;
        private final long duration;
        private final TimeUnit timeUnit;

        public Quota(int maxQuantity, long duration, TimeUnit timeUnit) {
            this.maxQuantity = maxQuantity;
            this.duration = duration;
            this.timeUnit = timeUnit;
        }

        public Quota(int maxQuantity, int duration, TimeUnit timeUnit) {
            this(maxQuantity, (long) duration, timeUnit);
        }

        public Long getDuration() {
            return duration;
        }

        public TimeUnit getTimeUnit() {
            return timeUnit;
        }

        public int getMaxQuantity() {
            return maxQuantity;
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof Quota) {
                return toString().equals(other.toString());
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return this.toString().hashCode();
        }

        @Override
        public String toString() {
            return maxQuantity + "_" + duration + "_" + timeUnit;
        }
    }

}
