package com.latticeengines.ldc_collectiondb.entity;

import javax.persistence.*;
import java.sql.Timestamp;

@Entity
@Table
public class CollectionWorker {
    @Id
    @Basic(optional = false)
    @Column(name = "WORKER_ID", unique = true, nullable = false)
    private String workerId;

    @Column(name = "VENDOR", nullable = false)
    private String vendor;

    @Column(name = "TASK_ARN", nullable = false)
    private String taskArn;

    @Column(name = "SPAWN_TIME", nullable = false)
    private Timestamp spawnTime;

    @Column(name = "TERMINATION_TIME", nullable = false)
    private Timestamp terminationTime;

    @Column(name = "STATUS", nullable = false)
    private String status;

    @Column(name = "RECORDS_COLLECTED", nullable = false)
    private long recordsCollected;

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public String getVendor() {
        return vendor;
    }

    public void setVendor(String vendor) {
        this.vendor = vendor;
    }

    public String getTaskArn() {
        return taskArn;
    }

    public void setTaskArn(String taskArn) {
        this.taskArn = taskArn;
    }

    public Timestamp getSpawnTime() {
        return spawnTime;
    }

    public void setSpawnTime(Timestamp spawnTime) {
        this.spawnTime = spawnTime;
    }

    public Timestamp getTerminationTime() {
        return terminationTime;
    }

    public void setTerminationTime(Timestamp terminationTime) {
        this.terminationTime = terminationTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getRecordsCollected() {
        return recordsCollected;
    }

    public void setRecordsCollected(long recordsCollected) {
        this.recordsCollected = recordsCollected;
    }
}
