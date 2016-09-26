package com.latticeengines.domain.exposed.dataflow.operations;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class BitCodeBook implements Serializable {

    private static final long serialVersionUID = -7220566464433207501L;
    private Map<String, Integer> bitsPosMap = new HashMap<>();
    private final Algorithm algorithm;

    public BitCodeBook(Algorithm algorithm) {
        this.algorithm = algorithm;
    }

    public void setBitsPosMap(Map<String, Integer> bitsPosMap) {
        this.bitsPosMap = bitsPosMap;
    }

    public Algorithm getAlgorithm() {
        return algorithm;
    }

    public Integer getBitPosForkey(String key) {
        return bitsPosMap.get(key);
    }

    public boolean hasKey(String key) {
        return bitsPosMap.keySet().contains(key);
    }

    public enum Algorithm {
        KEY_EXISTS
    }

}
