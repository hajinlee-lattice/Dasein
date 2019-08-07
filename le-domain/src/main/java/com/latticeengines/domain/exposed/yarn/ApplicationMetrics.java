package com.latticeengines.domain.exposed.yarn;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApplicationMetrics {

    @JsonProperty
    public String id;

    @JsonProperty
    public ResourceInfo resourceInfo;

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ResourceInfo {

        @JsonProperty
        public List<ResourcePartition> resourceUsagesByPartition;

    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ResourcePartition {

        @JsonProperty
        public String partitionName;

        @JsonProperty
        public Resource used;

        @JsonProperty
        public Resource reserved;

        @JsonProperty
        public Resource pending;

    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Resource {

        @JsonProperty
        public long memory;

        @JsonProperty
        public int vCores;

        // for jackson
        private Resource() {}

        Resource(long memory, int vCores) {
            this.memory = memory;
            this.vCores = vCores;
        }

        void add(Resource resource) {
            this.memory += resource.memory;
            this.vCores += resource.vCores;
        }

    }

    @JsonIgnore
    public Resource getPendingResource() {
        if (resourceInfo == null || CollectionUtils.isEmpty(resourceInfo.resourceUsagesByPartition)) {
            return new Resource(0, 0);
        } else {
            List<ResourcePartition> partitions = resourceInfo.resourceUsagesByPartition;
            Resource resource = new Resource(0, 0);
            for (ResourcePartition partition: partitions) {
                if (partition.pending != null) {
                    resource.add(partition.pending);
                }
            }
            return resource;
        }
    }
}
