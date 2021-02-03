package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RecordsStats {

    @JsonProperty("recordsFromLattice")
    private Long recordsFromLattice;

    @JsonProperty("recordsReceivedByTray")
    private Long recordsReceivedByTray;

    @JsonProperty("recordsToAcxiom")
    private Long recordsToAcxiom;

    @JsonProperty("recordsReceivedFromAcxiom")
    private Long recordsReceivedFromAcxiom;

    @JsonProperty("latticeRecordsSucceedToDestination")
    private Long latticeRecordsSucceedToDestination;

    @JsonProperty("latticeRecordsFailToDestination")
    private Long latticeRecordsFailToDestination;

    @JsonProperty("acxiomRecordsSucceedToDestination")
    private Long acxiomRecordsSucceedToDestination;

    @JsonProperty("acxiomRecordsFailToDestination")
    private Long acxiomRecordsFailToDestination;

    @JsonProperty("audienceSizeUpdate")
    private Long audienceSizeUpdate;

    public Long getRecordsFromLattice() {
        return recordsFromLattice;
    }

    public void setRecordsFromLattice(Long recordsFromLattice) {
        this.recordsFromLattice = recordsFromLattice;
    }

    public Long getRecordsReceivedByTray() {
        return recordsReceivedByTray;
    }

    public void setRecordsReceivedByTray(Long recordsReceivedByTray) {
        this.recordsReceivedByTray = recordsReceivedByTray;
    }

    public Long getRecordsToAcxiom() {
        return recordsToAcxiom;
    }

    public void setRecordsToAcxiom(Long recordsToAcxiom) {
        this.recordsToAcxiom = recordsToAcxiom;
    }

    public Long getRecordsReceivedFromAcxiom() {
        return recordsReceivedFromAcxiom;
    }

    public void setRecordsReceivedFromAcxiom(Long recordsReceivedFromAcxiom) {
        this.recordsReceivedFromAcxiom = recordsReceivedFromAcxiom;
    }

    public Long getLatticeRecordsSucceedToDestination() {
        return latticeRecordsSucceedToDestination;
    }

    public void setLatticeRecordsSucceedToDestination(Long latticeRecordsSucceedToDestination) {
        this.latticeRecordsSucceedToDestination = latticeRecordsSucceedToDestination;
    }

    public Long getLatticeRecordsFailToDestination() {
        return latticeRecordsFailToDestination;
    }

    public void setLatticeRecordsFailToDestination(Long latticeRecordsFailToDestination) {
        this.latticeRecordsFailToDestination = latticeRecordsFailToDestination;
    }

    public Long getAcxiomRecordsSucceedToDestination() {
        return acxiomRecordsSucceedToDestination;
    }

    public void setAcxiomRecordsSucceedToDestination(Long acxiomRecordsSucceedToDestination) {
        this.acxiomRecordsSucceedToDestination = acxiomRecordsSucceedToDestination;
    }

    public Long getAcxiomRecordsFailToDestination() {
        return acxiomRecordsFailToDestination;
    }

    public void setAcxiomRecordsFailToDestination(Long acxiomRecordsFailToDestination) {
        this.acxiomRecordsFailToDestination = acxiomRecordsFailToDestination;
    }

    public Long getAudienceSizeUpdate() {
        return audienceSizeUpdate;
    }

    public void setAudienceSizeUpdate(Long audienceSizeUpdate) {
        this.audienceSizeUpdate = audienceSizeUpdate;
    }

}
