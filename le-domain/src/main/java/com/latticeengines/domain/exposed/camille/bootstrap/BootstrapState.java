package com.latticeengines.domain.exposed.camille.bootstrap;

/**
 * Document representing the state of a particular service configuration in
 * either the CustomerSpaceService or Service scope.
 */
public class BootstrapState {
    public enum State {
        /**
         * Bootstrap has not run.
         */
        INITIAL,

        /**
         * Bootstrap has run and configured the service without error.
         */
        OK,

        /**
         * Bootstrap has run and failed to configure the service.
         */
        ERROR
    }

    /**
     * The state of this bootstrap configuration.
     */
    public State state;

    /**
     * The desired version of configuration for this service. Equals the
     * installedVersion if the previous operation succeeded. Otherwise equals
     * the desired configuration version.
     */
    public int desiredVersion;

    /**
     * The installed version of configuration for this service. If no
     * configuration has been installed, this equals -1.
     */
    public int installedVersion;

    /**
     * A detailed error message if the state == ERROR. Otherwise this is null.
     */
    public String errorMessage;
}
