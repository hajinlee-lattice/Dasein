package com.latticeengines.domain.exposed.datacloud.check;

//@formatter:off
public enum CheckCode {

    DuplicatedValue("Found duplicated value [%s] in the field [%s]."), //
    ExceededCount("Total count of records exceed [%s]."), //
    BelowExpectedCount("Total count of records below [%s]"), //
    EmptyField("Field [%s] should not be null or empty."), //
    UnderPopulatedField("Population of field [%s] is [%s] percent, lower than [%s] percent"), //
    IncompleteCoverageForCol("No record found for field [%s] with group value [%s]."), //
    OutOfCoverageValForRow("Row with id [%s] for field [%s] has value [%s] and it doesnt cover expected group values."), //
    DuplicatedValuesWithStatus("Found duplicated value [%s] in the the field [%s] for status [%s]"), //
    ExceededVersionDiffForDomOnly(
            "Number of Domain Only Records changes [%s] percent (from [%s] to [%s])"), //
    ExceededVersionDiffForNumOfBusinesses(
            "Number of Businesses changes [%s] percent (from [%s] to [%s]), more than [%s] percent");

    private final String msgPattern;

    CheckCode(String msgPattern) {
        this.msgPattern = msgPattern;
    }

    public String getMessage(Object... vals) {
        return String.format(this.msgPattern, vals);
    }

}
