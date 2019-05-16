package com.latticeengines.domain.exposed.cdl;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Convert;
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
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.ColumnEncryptorDecryptor;

@Entity
@Table(name = "ATLAS_DROPBOX", uniqueConstraints = {
        @UniqueConstraint(name = "UX_DROPBOX", columnNames = { "DROPBOX" }) })
@Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId")
public class DropBox implements HasPid, HasTenant {

    @JsonProperty("pid")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonProperty("dropbox")
    @Column(name = "DROPBOX", length = 8)
    private String dropBox;

    @JsonProperty("region")
    @Column(name = "REGION")
    private String region;

    @Enumerated(EnumType.STRING)
    @Column(name = "ACCESS_MODE", length = 20)
    private DropBoxAccessMode accessMode;

    @Column(name = "LATTICE_USER", length = 20)
    private String latticeUser;

    @Column(name = "EXTERNAL_ACCOUNT", length = 20)
    private String externalAccount;

    @Column(name = "ENCRYPTED_SECRET_KEY")
    @Convert(converter = ColumnEncryptorDecryptor.class)
    private String encryptedSecretKey;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public String getDropBox() {
        return dropBox;
    }

    public void setDropBox(String dropBox) {
        this.dropBox = dropBox;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public DropBoxAccessMode getAccessMode() {
        return accessMode;
    }

    public void setAccessMode(DropBoxAccessMode accessMode) {
        this.accessMode = accessMode;
    }

    public String getLatticeUser() {
        return latticeUser;
    }

    public void setLatticeUser(String latticeUser) {
        this.latticeUser = latticeUser;
    }

    public String getExternalAccount() {
        return externalAccount;
    }

    public void setExternalAccount(String externalAccount) {
        this.externalAccount = externalAccount;
    }

    public String getEncryptedSecretKey() {
        return encryptedSecretKey;
    }

    public void setEncryptedSecretKey(String encryptedSecretKey) {
        this.encryptedSecretKey = encryptedSecretKey;
    }
}
