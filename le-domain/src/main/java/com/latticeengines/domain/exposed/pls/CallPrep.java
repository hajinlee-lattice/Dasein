package com.latticeengines.domain.exposed.pls;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.Filter;

@Entity
@Table(name = "CALL_PREP")
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class CallPrep {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

}
