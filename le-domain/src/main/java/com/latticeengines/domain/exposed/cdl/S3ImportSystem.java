package com.latticeengines.domain.exposed.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "ATLAS_S3_IMPORT_SYSTEM", //
        indexes = { @Index(name = "IX_SYSTEM_NAME", columnList = "NAME") }, //
        uniqueConstraints = @UniqueConstraint(columnNames = { "TENANT_ID", "NAME" }))
@Filters({ @Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId") })
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class S3ImportSystem implements HasPid, HasName, HasTenant, HasTenantId {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonProperty("pid")
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("tenant")
    private Tenant tenant;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    @Column(name = "DISPLAY_NAME")
    @JsonProperty("display_name")
    private String displayName;

    @Column(name = "NAME", nullable = false)
    @JsonProperty("name")
    private String name;

    @Column(name = "SYSTEM_TYPE", length = 30, nullable = false)
    @JsonProperty("system_type")
    @Enumerated(EnumType.STRING)
    private SystemType systemType;

    @Column(name = "PRIORITY", nullable = false)
    @JsonProperty("priority")
    private int priority = Integer.MAX_VALUE;

    @Column(name = "ACCOUNT_SYSTEM_ID")
    @JsonProperty("account_system_id")
    private String accountSystemId;

    @Column(name = "CONTACT_SYSTEM_ID")
    @JsonProperty("contact_system_id")
    private String contactSystemId;

    @Column(name = "MAP_TO_LATTICE_ACCOUNT")
    @JsonProperty("map_to_lattice_account")
    private Boolean mapToLatticeAccount = false;

    @Column(name = "MAP_TO_LATTICE_CONTACT")
    @JsonProperty("map_to_lattice_contact")
    private Boolean mapToLatticeContact = false;

    @JsonProperty("secondary_account_ids")
    @Column(name = "SECONDARY_ACCOUNT_IDS", columnDefinition = "'JSON'")
    @Type(type = "json")
    private SecondaryIdList secondaryAccountIds;

    @JsonProperty("secondary_contact_ids")
    @Column(name = "SECONDARY_CONTACT_IDS", columnDefinition = "'JSON'")
    @Type(type = "json")
    private SecondaryIdList secondaryContactIds;

    @JsonProperty("tasks")
    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "importSystem")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private List<DataFeedTask> tasks = new ArrayList<>();

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
        if (tenant != null) {
            setTenantId(tenant.getPid());
        }
    }

    @Override
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public SystemType getSystemType() {
        return systemType;
    }

    public void setSystemType(SystemType systemType) {
        this.systemType = systemType;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    @JsonProperty("is_primary_system")
    public boolean isPrimarySystem() {
        return (priority == 1) && (Boolean.TRUE.equals(mapToLatticeAccount));
    }

    public String getAccountSystemId() {
        return accountSystemId;
    }

    public void setAccountSystemId(String accountSystemId) {
        this.accountSystemId = accountSystemId;
    }

    public String getContactSystemId() {
        return contactSystemId;
    }

    public void setContactSystemId(String contactSystemId) {
        this.contactSystemId = contactSystemId;
    }

    @JsonIgnore
    public String generateAccountSystemId() {
        return String.format("user_%s_%s_AccountId", name, RandomStringUtils.randomAlphanumeric(8).toLowerCase());
    }

    @JsonIgnore
    public String generateContactSystemId() {
        return String.format("user_%s_%s_ContactId", name, RandomStringUtils.randomAlphanumeric(8).toLowerCase());
    }

    public Boolean isMapToLatticeAccount() {
        return mapToLatticeAccount;
    }

    public void setMapToLatticeAccount(Boolean mapToLatticeAccount) {
        this.mapToLatticeAccount = mapToLatticeAccount;
    }

    public Boolean isMapToLatticeContact() {
        return mapToLatticeContact;
    }

    public void setMapToLatticeContact(Boolean mapToLatticeContact) {
        this.mapToLatticeContact = mapToLatticeContact;
    }

    public SecondaryIdList getSecondaryAccountIds() {
        return secondaryAccountIds;
    }

    public String getSecondaryAccountId(EntityType entityType) {
        if (secondaryAccountIds != null) {
            return secondaryAccountIds.getSecondaryId(entityType);
        }
        return StringUtils.EMPTY;
    }

    public void setSecondaryAccountIds(SecondaryIdList secondaryAccountIds) {
        this.secondaryAccountIds = secondaryAccountIds;
    }

    public void addSecondaryAccountId(EntityType entityType, String secondaryAccountId) {
        if (secondaryAccountIds == null) {
            secondaryAccountIds = new SecondaryIdList();
        }
        secondaryAccountIds.addSecondaryId(entityType, secondaryAccountId);
    }

    @JsonIgnore
    public List<String> getSecondaryAccountIdsSortByPriority() {
        if (secondaryAccountIds != null) {
            return secondaryAccountIds.getSecondaryIds(SecondaryIdList.SortBy.PRIORITY);
        }
        return Collections.emptyList();
    }

    public SecondaryIdList getSecondaryContactIds() {
        return secondaryContactIds;
    }

    public String getSecondaryContactId(EntityType entityType) {
        if (secondaryContactIds != null) {
            return secondaryContactIds.getSecondaryId(entityType);
        }
        return StringUtils.EMPTY;
    }

    public void setSecondaryContactIds(SecondaryIdList secondaryContactIds) {
        this.secondaryContactIds = secondaryContactIds;
    }

    public void addSecondaryContactId(EntityType entityType, String secondaryContactId) {
        if (secondaryContactIds == null) {
            secondaryContactIds = new SecondaryIdList();
        }
        secondaryContactIds.addSecondaryId(entityType, secondaryContactId);
    }

    @JsonIgnore
    public List<String> getSecondaryContactIdsSortByPriority() {
        if (secondaryContactIds != null) {
            return secondaryContactIds.getSecondaryIds(SecondaryIdList.SortBy.PRIORITY);
        }
        return Collections.emptyList();
    }

    public List<DataFeedTask> getTasks() {
        return tasks;
    }

    public void setTasks(List<DataFeedTask> tasks) {
        this.tasks = tasks;
    }

    public enum SystemType {
        Salesforce {
            @Override
            public Collection<EntityType> getEntityTypes() {
                return Arrays.asList(EntityType.Accounts, EntityType.Contacts,
                        EntityType.Leads);
            }
            @Override
            public String getDefaultSystemName() {
                return "Default_Salesforce_System";
            }
        },
        Pardot {
            @Override
            public Collection<EntityType> getEntityTypes() {
                return Collections.singletonList(EntityType.Leads);
            }

            @Override
            public EntityType getPrimaryContact() {
                return EntityType.Leads;
            }

            @Override
            public String getDefaultSystemName() {
                return "Default_Pardot_System";
            }
        },
        Marketo {
            @Override
            public Collection<EntityType> getEntityTypes() {
                return Collections.singletonList(EntityType.Leads);
            }

            @Override
            public EntityType getPrimaryContact() {
                return EntityType.Leads;
            }
            @Override
            public String getDefaultSystemName() {
                return "Default_Marketo_System";
            }
        },
        Eloqua {
            @Override
            public Collection<EntityType> getEntityTypes() {
                return Collections.singletonList(EntityType.Leads);
            }

            @Override
            public EntityType getPrimaryContact() {
                return EntityType.Leads;
            }
            @Override
            public String getDefaultSystemName() {
                return "Default_Eloqua_System";
            }
        },
        GoogleAnalytics {
            @Override
            public Collection<EntityType> getEntityTypes() {
                return Arrays.asList(EntityType.Accounts, EntityType.Contacts);
            }
            @Override
            public String getDefaultSystemName() {
                return "Default_GoogleAnalytics_System";
            }
        },
        Website {
            @Override
            public Collection<EntityType> getEntityTypes() {
                return Arrays.asList(EntityType.WebVisit, EntityType.WebVisitPathPattern, EntityType.WebVisitSourceMedium);
            }
            @Override
            public String getDefaultSystemName() {
                return "Default_Website_System";
            }
        },
        DCP {
            @Override
            public Collection<EntityType> getEntityTypes() {
                return Collections.emptyList();
            }
            @Override
            public String getDefaultSystemName() {
                return "Default_DCP_System";
            }
        },
        DnbIntent {
            @Override
            public Collection<EntityType> getEntityTypes() {
                return Collections.singletonList(EntityType.CustomIntent);
            }
            @Override
            public String getDefaultSystemName() {
                return "Default_DnbIntent_System";
            }
        },
        Other;

        public Collection<EntityType> getEntityTypes() {
            return Arrays.asList(EntityType.Accounts, EntityType.Contacts, EntityType.ProductPurchases,
                    EntityType.ProductBundles, EntityType.ProductHierarchy);
        }

        public EntityType getPrimaryAccount() {
            return EntityType.Accounts;
        }

        public EntityType getPrimaryContact() {
            return EntityType.Contacts;
        }

        public String getDefaultSystemName() { return "Default_System"; }
    }
}
