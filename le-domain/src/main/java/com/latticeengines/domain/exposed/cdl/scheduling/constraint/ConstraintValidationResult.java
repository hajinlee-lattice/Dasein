package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

public class ConstraintValidationResult {
    public static final ConstraintValidationResult VALID = new ConstraintValidationResult(false, null);

    private final boolean violated;
    private final String reason;

    public ConstraintValidationResult(boolean violated, String reason) {
        this.violated = violated;
        this.reason = reason;
    }

    public boolean isViolated() {
        return violated;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public String toString() {
        return "ConstraintValidationResult{" + "violated=" + violated + ", reason='" + reason + '\'' + '}';
    }
}
