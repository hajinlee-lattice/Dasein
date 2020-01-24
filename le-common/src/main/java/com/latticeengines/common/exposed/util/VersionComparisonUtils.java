package com.latticeengines.common.exposed.util;

public final class VersionComparisonUtils {

    protected VersionComparisonUtils() {
        throw new UnsupportedOperationException();
    }

    public static int compareVersion(String version, String otherVersion) {
        String[] versionNumbers = version.split("\\.");
        String[] otherVersionNumbers = otherVersion.split("\\.");
        int i = 0, j = 0;
        while (i < versionNumbers.length && j < otherVersionNumbers.length) {
            int n1 = Integer.parseInt(versionNumbers[i++]);
            int n2 = Integer.parseInt(otherVersionNumbers[j++]);
            if (n1 == n2) {
                continue;
            }
            return n1 < n2 ? -1 : 1;
        }
        while (i != versionNumbers.length) {
            int n1 = Integer.parseInt(versionNumbers[i++]);
            if (n1 != 0) {
                return 1;
            }
        }
        while (j != otherVersionNumbers.length) {
            int n2 = Integer.parseInt(otherVersionNumbers[j++]);
            if (n2 != 0) {
                return -1;
            }
        }
        return 0;
    }

}
