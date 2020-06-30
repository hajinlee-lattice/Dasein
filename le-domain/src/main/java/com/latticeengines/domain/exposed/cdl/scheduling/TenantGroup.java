package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

/**
 * Group of tenants that share common PA quota
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TenantGroup {

    private final String groupName;
    private final int quota;
    private final Set<String> tenantIds;
    private final Set<String> runningOrScheduledTenantIds = new HashSet<>();

    @JsonCreator
    public TenantGroup(@JsonProperty("group_name") String groupName, @JsonProperty("quota") int quota,
            @JsonProperty("tenant_ids") Set<String> tenantIds) {
        Preconditions.checkNotNull(groupName);
        Preconditions.checkArgument(quota > 0, "Quota should be greater than 0");
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(tenantIds), "Tenants in group should not be empty");
        this.groupName = groupName;
        this.quota = quota;
        this.tenantIds = ImmutableSet.copyOf(tenantIds.stream() //
                .filter(StringUtils::isNotBlank) //
                .map(id -> CustomerSpace.parse(id).toString()) //
                .collect(Collectors.toList()));
    }

    @JsonProperty("group_name")
    public String getGroupName() {
        return groupName;
    }

    @JsonProperty("quota")
    public int getQuota() {
        return quota;
    }

    @JsonProperty("tenant_ids")
    public Set<String> getTenantIds() {
        return tenantIds;
    }

    @JsonProperty("running_or_scheduled_tenant_ids")
    public Set<String> getRunningOrScheduledTenantIds() {
        return ImmutableSet.copyOf(runningOrScheduledTenantIds);
    }

    /*-
     * whether target tenant has reach quota limit in any tenant group.
     * return false if tenant is not in any group
     */
    public boolean reachQuotaLimit(@NotNull String tenantId) {
        if (!tenantIds.contains(tenantId) || runningOrScheduledTenantIds.contains(tenantId)) {
            return false;
        }

        return runningOrScheduledTenantIds.size() + 1 > quota;
    }

    public boolean addTenant(@NotNull String tenantId) {
        if (reachQuotaLimit(tenantId) || !tenantIds.contains(tenantId)) {
            return false;
        }

        return runningOrScheduledTenantIds.add(tenantId);
    }

    public boolean removeTenant(@NotNull String tenantId) {
        return runningOrScheduledTenantIds.remove(tenantId);
    }

    @Override
    public String toString() {
        return "TenantGroup{" + "groupName='" + groupName + '\'' + ", quota=" + quota + ", tenantIds=" + tenantIds
                + ", runningOrScheduledTenantIds=" + runningOrScheduledTenantIds + '}';
    }
}
