package com.latticeengines.domain.exposed.cdl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "CDL_EXTERNAL_SYSTEM")
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class CDLExternalSystem implements HasPid, HasTenant, HasTenantId {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("tenant")
    private Tenant tenant;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    @JsonProperty("crm")
    @Column(name = "CRM")
    @Enumerated(EnumType.STRING)
    private CRMType CRM;

    @JsonProperty("map")
    @Column(name = "MAP")
    @Enumerated(EnumType.STRING)
    private MAPType MAP;

    @JsonProperty("erp")
    @Column(name = "ERP")
    @Enumerated(EnumType.STRING)
    private ERPType ERP;


    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
        if (tenant != null) {
            setTenantId(tenant.getPid());
        }
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    public CRMType getCRM() {
        return CRM;
    }

    public void setCRM(CRMType CRM) {
        this.CRM = CRM;
    }

    public MAPType getMAP() {
        return MAP;
    }

    public void setMAP(MAPType MAP) {
        this.MAP = MAP;
    }

    public ERPType getERP() {
        return ERP;
    }

    public void setERP(ERPType ERP) {
        this.ERP = ERP;
    }

    public enum CRMType {
        SFDC_Sandbox("SFDC Sandbox", InterfaceName.SalesforceSandboxAccountID, InterfaceName.SalesforceSandboxContactID),
        SFDC_Production("SFDC Production", InterfaceName.SalesforceAccountID, InterfaceName.SalesforceContactID);

        private final String systemName;
        private final InterfaceName accountInterface;
        private final InterfaceName contactInterface;
        private static Map<String, CRMType> nameMap;
        private static Map<InterfaceName, CRMType> accountInterfaceMap;

        static {
            nameMap = new HashMap<>();
            accountInterfaceMap = new HashMap<>();
            for (CRMType crm : CRMType.values()) {
                nameMap.put(crm.name(), crm);
                accountInterfaceMap.put(crm.accountInterface, crm);
            }
        }

        CRMType(String systemName, InterfaceName accountInterface, InterfaceName contactInterface) {
            this.systemName = systemName;
            this.accountInterface = accountInterface;
            this.contactInterface = contactInterface;
        }

        public static CRMType fromName(String systemName) {
            if (StringUtils.isEmpty(systemName)) {
                return null;
            }
            if (nameMap.containsKey(systemName)) {
                return nameMap.get(systemName);
            } else {
                throw new IllegalArgumentException("Cannot find a CRM system with name" + systemName);
            }
        }

        public static CRMType fromAccountInterface(InterfaceName interfaceName) {
            if (interfaceName == null) {
                return null;
            }
            if (accountInterfaceMap.containsKey(interfaceName)) {
                return accountInterfaceMap.get(interfaceName);
            } else {
                throw new IllegalArgumentException("Cannot find a CRM system with account interface " + interfaceName
                        .name());
            }
        }

        @JsonValue
        public InterfaceName getAccountInterface() {
            return accountInterface;
        }
    }

    public enum MAPType {
        Marketo("Marketo", InterfaceName.MarketoAccountID, null),
        Eloqua("Eloqua", InterfaceName.EloquaAccountID, null);

        private final String systemName;
        private final InterfaceName accountInterface;
        private final InterfaceName contactInterface;
        private static Map<String, MAPType> nameMap;
        private static Map<InterfaceName, MAPType> accountInterfaceMap;

        static {
            nameMap = new HashMap<>();
            accountInterfaceMap = new HashMap<>();
            for(MAPType map : MAPType.values()) {
                nameMap.put(map.name(), map);
                accountInterfaceMap.put(map.accountInterface, map);
            }
        }

        MAPType(String systemName, InterfaceName accountInterface, InterfaceName contactInterface) {
            this.systemName = systemName;
            this.accountInterface = accountInterface;
            this.contactInterface = contactInterface;
        }


        public static MAPType fromName(String systemName) {
            if (StringUtils.isEmpty(systemName)) {
                return null;
            }
            if (nameMap.containsKey(systemName)) {
                return nameMap.get(systemName);
            } else {
                throw new IllegalArgumentException("Cannot find a MAP system with name" + systemName);
            }
        }

        public static MAPType fromAccountInterface(InterfaceName interfaceName) {
            if (interfaceName == null) {
                return null;
            }
            if (accountInterfaceMap.containsKey(interfaceName)) {
                return accountInterfaceMap.get(interfaceName);
            } else {
                throw new IllegalArgumentException("Cannot find a MAP system with account interface " + interfaceName
                        .name());
            }
        }

        @JsonValue
        public InterfaceName getAccountInterface() {
            return accountInterface;
        }
    }

    public enum ERPType {
        SomeType("SomeType", InterfaceName.AccountId, InterfaceName.ContactId);

        private final String systemName;
        private final InterfaceName accountInterface;
        private final InterfaceName contactInterface;
        private static Map<String, ERPType> nameMap;
        private static Map<InterfaceName, ERPType> accountInterfaceMap;

        static {
            nameMap = new HashMap<>();
            accountInterfaceMap = new HashMap<>();
            for (ERPType erp: ERPType.values()) {
                nameMap.put(erp.name(), erp);
                accountInterfaceMap.put(erp.accountInterface, erp);
            }
        }

        ERPType(String systemName, InterfaceName accountInterface, InterfaceName contactInterface) {
            this.systemName = systemName;
            this.accountInterface = accountInterface;
            this.contactInterface = contactInterface;
        }

        public static ERPType fromName(String systemName) {
            if (StringUtils.isEmpty(systemName)) {
                return null;
            }
            if (nameMap.containsKey(systemName)) {
                return nameMap.get(systemName);
            } else {
                throw new IllegalArgumentException("Cannot find a ERP system with name" + systemName);
            }
        }

        public static ERPType fromAccountInterface(InterfaceName interfaceName) {
            if (interfaceName == null) {
                return null;
            }
            if (accountInterfaceMap.containsKey(interfaceName)) {
                return accountInterfaceMap.get(interfaceName);
            } else {
                throw new IllegalArgumentException("Cannot find a ERP system with account interface " + interfaceName
                        .name());
            }
        }

        @JsonValue
        public InterfaceName getAccountInterface() {
            return accountInterface;
        }
    }

    @JsonIgnore
    @Transient
    public static Map<String, List<Enum>> EXTERNAL_SYSTEM = new HashMap<>();
    static {
        EXTERNAL_SYSTEM.put("CRM", new ArrayList<>());
        for (CRMType crm : CRMType.values()) {
            EXTERNAL_SYSTEM.get("CRM").add(crm);
        }
        EXTERNAL_SYSTEM.put("MAP", new ArrayList<>());
        for (MAPType map : MAPType.values()) {
            EXTERNAL_SYSTEM.get("MAP").add(map);
        }
        EXTERNAL_SYSTEM.put("ERP", new ArrayList<>());
        for (ERPType erp : ERPType.values()) {
            EXTERNAL_SYSTEM.get("ERP").add(erp);
        }

    }
}
