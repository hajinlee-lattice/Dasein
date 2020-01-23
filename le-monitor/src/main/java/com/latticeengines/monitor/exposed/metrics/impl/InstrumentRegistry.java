package com.latticeengines.monitor.exposed.metrics.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.monitor.exposed.annotation.InvocationInstrument;

public final class InstrumentRegistry {

    protected InstrumentRegistry() {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(InstrumentRegistry.class);

    public static final String DEFAULT = "default";

    private static final InvocationInstrument DEFAULT_INSTRUMENT = new InvocationInstrument() {};

    private static final ConcurrentMap<String, InvocationInstrument> instruments = new ConcurrentHashMap<>();

    static InvocationInstrument getInstrument(String instrumentName) {
        if (DEFAULT.equals(instrumentName)) {
            return DEFAULT_INSTRUMENT;
        } else {
            return instruments.getOrDefault(instrumentName, DEFAULT_INSTRUMENT);
        }
    }

    public static void register(String instrumentName, InvocationInstrument instrument) {
        instruments.putIfAbsent(instrumentName, instrument);
        log.info("Registered an instrument named {}: {}", instrumentName, instrument);
    }

}
