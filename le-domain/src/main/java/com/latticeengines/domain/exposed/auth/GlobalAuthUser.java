package com.latticeengines.domain.exposed.auth;

import java.util.List;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "GlobalUser", indexes = { @Index(name = "IX_EMAIL", columnList = "Email") })
public class GlobalAuthUser extends BaseGlobalAuthObject implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @JsonIgnore
    @Column(name = "GlobalUser_ID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("email")
    @Column(name = "Email", nullable = true)
    private String email;

    @JsonProperty("first_name")
    @Column(name = "First_Name", nullable = true)
    private String firstName;

    @JsonProperty("last_name")
    @Column(name = "Last_Name", nullable = true)
    private String lastName;

    @JsonProperty("title")
    @Column(name = "Title", nullable = true)
    private String title;

    @JsonProperty("phone_number")
    @Column(name = "Phone_Number", nullable = true)
    private String phoneNumber;

    @JsonProperty("isActive")
    @Column(name = "IsActive", nullable = true)
    private boolean isActive;

    @JsonProperty("created_by_user")
    @Column(name = "Created_By_User", nullable = true)
    private String createdByUser;

    @JsonProperty("invalid_login_attempts")
    @Column(name = "InvalidLoginAttempts")
    private int invalidLoginAttempts = 0;

    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "globalAuthUser")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private List<GlobalAuthAuthentication> gaAuthentications;

    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "globalAuthUser")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private List<GlobalAuthUserTenantRight> gaUserTenantRights;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;

    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public boolean getIsActive() {
        return isActive;
    }

    public void setIsActive(boolean isActive) {
        this.isActive = isActive;
    }

    public String getCreatedByUser() { return createdByUser; }

    public void setCreatedByUser(String createdByUser) { this.createdByUser = createdByUser; }

    public List<GlobalAuthAuthentication> getAuthentications() {
        return gaAuthentications;
    }

    public void setAuthentications(List<GlobalAuthAuthentication> gaAuthentications) {
        this.gaAuthentications = gaAuthentications;
    }

    public List<GlobalAuthUserTenantRight> getUserTenantRights() {
        return gaUserTenantRights;
    }

    public void setUserTenantRights(List<GlobalAuthUserTenantRight> gaUserTenantRights) {
        this.gaUserTenantRights = gaUserTenantRights;
    }

    public int getInvalidLoginAttempts() {
        return invalidLoginAttempts;
    }

    public void setInvalidLoginAttempts(int invalidLoginAttempts) {
        this.invalidLoginAttempts = invalidLoginAttempts;
    }
}
