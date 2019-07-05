package com.latticeengines.actors.exposed.traveler;

public class TravelException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 2300790949768322121L;

    public TravelException(String msg) {
        super(msg);
    }

    public TravelException(String msg, Throwable throwable) {
        super(msg, throwable);
    }

}
