package com.latticeengines.domain.exposed.camille.bootstrap;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.VersionedDocument;

/**
 * Document representing the state of a particular service configuration in
 * either the CustomerSpaceService or Service scope.
 */
public class BootstrapState extends VersionedDocument {
    public static BootstrapState createInitialState() {
        BootstrapState toReturn = new BootstrapState();
        toReturn.state = State.INITIAL;
        toReturn.desiredVersion = -1;
        toReturn.installedVersion = -1;
        toReturn.errorMessage = null;
        return toReturn;
    }

    public static BootstrapState constructOKState(int version) {
        BootstrapState toReturn = new BootstrapState();
        toReturn.state = State.OK;
        toReturn.desiredVersion = version;
        toReturn.installedVersion = version;
        toReturn.errorMessage = null;
        return toReturn;
    }

    public static BootstrapState constructErrorState(int desiredVersion, int installedVersion, String errorMessage) {
        BootstrapState toReturn = new BootstrapState();
        toReturn.state = State.ERROR;
        toReturn.desiredVersion = desiredVersion;
        toReturn.installedVersion = installedVersion;
        toReturn.errorMessage = errorMessage;
        return toReturn;
    }

    public static BootstrapState constructMigratedState() {
        BootstrapState toReturn = new BootstrapState();
        toReturn.state = State.MIGRATED;
        toReturn.desiredVersion = -1;
        toReturn.installedVersion = -1;
        toReturn.errorMessage = "This component was migrated from an earlier version.";
        return toReturn;
    }

    public static BootstrapState constructDeletingState() {
        BootstrapState toReturn = new BootstrapState();
        toReturn.state = State.UNINSTALLING;
        toReturn.desiredVersion = -1;
        toReturn.installedVersion = -1;
        toReturn.errorMessage = null;
        return toReturn;
    }

    public static BootstrapState constructDeletedState(int version) {
        BootstrapState toReturn = new BootstrapState();
        toReturn.state = State.UNINSTALLED;
        toReturn.desiredVersion = version;
        toReturn.installedVersion = version;
        toReturn.errorMessage = null;
        return toReturn;
    }

    public BootstrapState() {
    }

    public enum State {
        /**
         * Bootstrap has never been run. The tenant was migrated from an earlier version.
         * None of component level configuration is known.
         */
        MIGRATED,

        /**
         * Bootstrap has not run.
         */
        INITIAL,

        /**
         * Bootstrap has run and configured the service without error.
         */
        OK,

        /**
         * Bootstrap is going to send delete message to destroyer.
         */
        UNINSTALLING,

        /**
         * Bootstrap has run the destroyer.
         */
        UNINSTALLED,

        /**
         * Bootstrap has run and failed to configure the service.
         */
        ERROR
    }

    @Override
    public int hashCode() {
        // Don't include base class version field - no nice way to do this.
        return HashCodeBuilder.reflectionHashCode(17, 37, this, false);
    }

    @Override
    public boolean equals(Object obj) {
        // Don't include base class version field - no nice way to do this.
        return EqualsBuilder.reflectionEquals(this, obj, false, getClass());
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
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
