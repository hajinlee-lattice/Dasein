package com.latticeengines.upgrade.yarn;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public class YarnPathUtils {

    private YarnPathUtils() {}

    public static String parseEventTable(String hdfsPath) {
        hdfsPath = stripoutProtocal(hdfsPath);
        String[] tokens = hdfsPath.split("/");
        return tokens[6];
    }

    public static String parseContainerId(String hdfsPath) {
        hdfsPath = stripoutProtocal(hdfsPath);
        String[] tokens = hdfsPath.split("/");
        return tokens[8];
    }

    public static String parseUuid(String hdfsPath) {
        hdfsPath = stripoutProtocal(hdfsPath);
        String[] tokens = hdfsPath.split("/");
        return tokens[7];
    }

    public static String constructModelGuidFromUuid(String uuid) {
        return "ms__" + uuid + "-PLSModel";
    }

    public static String constructTupleIdCustomerRoot(String customerBase, String customer) {
        return customerBase + "/" + CustomerSpace.parse(customer);
    }

    public static String constructTupleIdModelsRoot(String customerBase, String customer) {
        return constructTupleIdCustomerRoot(customerBase, customer) + "/models";
    }

    public static String constructTupleIdDataRoot(String customerBase, String customer) {
        return constructTupleIdCustomerRoot(customerBase, customer) + "/data";
    }

    public static String constructSingularIdCustomerRoot(String customerBase, String customer) {
        return customerBase + "/" + CustomerSpace.parse(customer).getTenantId();
    }

    public static String constructSingularIdModelsRoot(String customerBase, String customer) {
        return constructSingularIdCustomerRoot(customerBase, customer) + "/models";
    }

    public static String constructSingularIdDataRoot(String customerBase, String customer) {
        return constructSingularIdCustomerRoot(customerBase, customer) + "/data";
    }

    public static String substituteByTupleId(String path) {
        Pattern pattern = Pattern.compile("/customers/([^/$]*)");
        Matcher matcher = pattern.matcher(path);
        if (matcher.find()) {
            String customer = matcher.group(0).split("/")[2];
            return path.replaceFirst(customer, CustomerSpace.parse(customer).toString());
        }
        throw new IllegalArgumentException("No customer token can be found from the path " + path);
    }

    public static String substituteBySingularId(String path) {
        Pattern pattern = Pattern.compile("/customers/([^/$]*)");
        Matcher matcher = pattern.matcher(path);
        if (matcher.find()) {
            String customer = matcher.group(0).split("/")[2];
            return path.replaceFirst(customer, CustomerSpace.parse(customer).getTenantId());
        }
        throw new IllegalArgumentException("No customer token can be found from the path " + path);
    }

    public static String extractUuid(String modelGuid) {
        Pattern pattern = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
        Matcher matcher = pattern.matcher(modelGuid);
        if (matcher.find()) {
            return matcher.group(0);
        }
        throw new IllegalArgumentException("Cannot find uuid pattern in the model GUID " + modelGuid);
    }

    private static String stripoutProtocal(String hdfsPath) {
        return hdfsPath.replaceFirst("[^:]*://[^/]*/", "/");
    }

}
