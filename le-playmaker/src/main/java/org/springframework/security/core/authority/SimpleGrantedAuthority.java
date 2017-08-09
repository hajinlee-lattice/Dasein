package org.springframework.security.core.authority;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.util.Assert;

public final class SimpleGrantedAuthority implements GrantedAuthority {
    private static final long serialVersionUID = 320L;
    private final String role;

    public SimpleGrantedAuthority(String role) {
        Assert.hasText(role, "A granted authority textual representation is required");
        this.role = role;
    }

    public String getAuthority() {
        return this.role;
    }

    public boolean equals(Object obj) {
        return this == obj?true:(obj instanceof SimpleGrantedAuthority?this.role.equals(((SimpleGrantedAuthority)obj).role):false);
    }

    public int hashCode() {
        return this.role.hashCode();
    }

    public String toString() {
        return this.role;
    }
}
