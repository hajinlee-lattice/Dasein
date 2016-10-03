package com.latticeengines.domain.exposed.dataflow.operations;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

public class BitCodeBook implements Serializable {

    private static final long serialVersionUID = -7220566464433207501L;
    private ImmutableMap<String, Integer> bitsPosMap;
    private final Algorithm encodeAlgo;
    private final DecodeStrategy decodeStrategy;

    public BitCodeBook(Algorithm encodeAlgo) {
        this.encodeAlgo = encodeAlgo;
        this.decodeStrategy = null;
    }

    public BitCodeBook(DecodeStrategy decodeStrategy) {
        this.decodeStrategy = decodeStrategy;
        this.encodeAlgo = null;
    }

    public void setBitsPosMap(Map<String, Integer> bitsPosMap) {
        Map<String, Integer> copy = new HashMap<>();
        if (bitsPosMap != null) {
            for (Map.Entry<String, Integer> entry: bitsPosMap.entrySet()) {
                copy.put(entry.getKey(), entry.getValue());
            }
        }
        this.bitsPosMap = ImmutableMap.copyOf(copy);
    }

    public Algorithm getEncodeAlgo() {
        return encodeAlgo;
    }

    public DecodeStrategy getDecodeStrategy() {
        return decodeStrategy;
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

    public enum DecodeStrategy {
        BOOLEAN_YESNO
    }

}
