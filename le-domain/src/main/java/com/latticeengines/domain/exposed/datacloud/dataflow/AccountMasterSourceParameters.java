package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccountMasterSourceParameters {

    private static final Logger log = LoggerFactory.getLogger(AccountMasterSourceParameters.class);

    public static final int DomainBased = 1;
    public static final int DunsBased = 2;

    private String sourceName;
    private int sourceType = 0;

    private String joinKey = null;
    private String secondKey = null;

    private List<String> sourceAttrs = new ArrayList<String>();
    private List<String> outputAttrs = new ArrayList<String>();

    public String getSourceName() { return sourceName; }

    public void setSourceName(String sourceName) { this.sourceName = sourceName; }

    public int getSourceType() { return sourceType; }

    public void setSourceType(int sourceType) { this.sourceType = sourceType; }

    public List<String> getSourceAttrs() { return sourceAttrs; }

    public List<String> getOutputAttrs() { return outputAttrs; }

    public void addAttribute(String sourceAttr, String outputAttr) {
        if ((joinKey != null) && joinKey.equals(sourceAttr)) {
            return;
        }
        if ((secondKey != null) && secondKey.equals(sourceAttr)) {
            return;
        }
        this.sourceAttrs.add(sourceAttr);
        this.outputAttrs.add(outputAttr);
    }

    public void setJoinKey(String joinKey) {
        this.joinKey = joinKey;
    }

    public void setSecondKey(String secondKey) {
        this.secondKey = secondKey;
    }

    public String getJoinKey() {
        return joinKey;
    }

    public String getSecondKey() {
        return secondKey;
    }

    public void filterAttrs(List<String> fieldNames) {
        HashSet<String> fieldSet = new HashSet<String>(fieldNames);
        List<String> fileteredSourceAttrs = new ArrayList<String>();
        List<String> fileteredOutputAttrs = new ArrayList<String>();
        for (int i = 0; i < sourceAttrs.size(); i++) {
            String sourceAttr = sourceAttrs.get(i);
            if (fieldSet.contains(sourceAttr)) {
                String outputAttr = outputAttrs.get(i);
                fileteredSourceAttrs.add(sourceAttr);
                fileteredOutputAttrs.add(outputAttr);
                log.info("Source " + sourceName + " filtered " + sourceAttr + " " + outputAttr);
            } else {
                log.info("Source " + sourceName + " attribute " + sourceAttr + "not in avro");
            }
        }
        sourceAttrs = fileteredSourceAttrs;
        outputAttrs = fileteredOutputAttrs;
    }
}
