package com.latticeengines.camille.config.bootstrap;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public class VersionMismatchException extends Exception {
    private CustomerSpace space;
    private String serviceName;
    private int dataVersion;
    private int executableVersion;

    private static final long serialVersionUID = 1L;

    public VersionMismatchException(CustomerSpace space, String serviceName, int dataVersion, int executableVersion) {
        this.space = space;
        this.serviceName =  serviceName;
        this.dataVersion = dataVersion;
        this.executableVersion = executableVersion;
    }

    @Override
    public String toString() {
        return getMessage();
    }

    @Override
    public String getMessage() {
        String message = String.format("Version of data for service %s on customerspace %s is on version %d but this executable expects version %d.",
                serviceName,
                space,
                dataVersion,
                executableVersion);
        return message;
    }
}
