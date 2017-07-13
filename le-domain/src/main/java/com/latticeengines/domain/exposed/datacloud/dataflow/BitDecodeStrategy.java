package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BitDecodeStrategy implements Serializable {

    private static final long serialVersionUID = -1L;

    public static final String BOOLEAN_YESNO = "BOOLEAN_YESNO";

    @JsonProperty("EncodedColumn")
    private String encodedColumn;

    @JsonProperty("BitPosition")
    private int bitPosition;

    @JsonProperty("BitInterpretation")
    private String bitInterpretation;

    @JsonProperty("BitUnit")
    private Integer bitUnit;

    @JsonProperty("ValueDict")
    private String valueDict;

    public String getEncodedColumn() {
        return encodedColumn;
    }

    public void setEncodedColumn(String encodedColumn) {
        this.encodedColumn = encodedColumn;
    }

    public int getBitPosition() {
        return bitPosition;
    }

    public void setBitPosition(int bitPosition) {
        this.bitPosition = bitPosition;
    }

    public String getBitInterpretation() {
        return bitInterpretation;
    }

    public void setBitInterpretation(String bitInterpretation) {
        this.bitInterpretation = bitInterpretation;
    }

    public Integer getBitUnit() {
        return bitUnit;
    }

    public void setBitUnit(Integer bitUnit) {
        this.bitUnit = bitUnit;
    }

    public String getValueDict() {
        return valueDict;
    }

    public void setValueDict(String valueDict) {
        this.valueDict = valueDict;
    }

    @JsonIgnore
    public String codeBookKey() {
        return getEncodedColumn() + "_" + getBitInterpretation();
    }
}
