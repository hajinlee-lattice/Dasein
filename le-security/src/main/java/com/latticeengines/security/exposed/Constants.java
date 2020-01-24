package com.latticeengines.security.exposed;

public final class Constants {

    protected Constants() {
        throw new UnsupportedOperationException();
    }
    public static final String INTERNAL_SERVICE_HEADERNAME = "MagicAuthentication";
    public static final String INTERNAL_SERVICE_HEADERVALUE = "Security through obscurity!";
    public static final String PASSWORD_UPDATE_FORMAT_HEADERNAME = "X-LE-UpdateFormat";
    public static final String AUTHORIZATION = "Authorization";
    public static final String TENANT_ID = "TenantId";
    public static final String LATTICE_SECRET_KEY_HEADERNAME = "Lattice-Secret-Key";
}
