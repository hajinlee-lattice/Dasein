package com.latticeengines.datacloud.collection.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CollectionDBUtil {
    public static final String VENDOR_ALEXA = "ALEXA";
    public static final String VENDOR_BUILTWITH = "BUILTWITH";
    public static final String VENDOR_COMPETE = "COMPETE";
    public static final String VENDOR_FEATURE = "FEATURE";
    public static final String VENDOR_HPA_NEW = "HPA_NEW";
    public static final String VENDOR_ORBI_V2 = "ORBINTELLIGENCEV2";
    public static final String VENDOR_SEMRUSH = "SEMRUSH";
    public static final int VENDOR_COUNT = 7;
    public static final int DEF_COLLECTION_BATCH = 8192;
    public static final int DEF_MAX_RETRIES = 3;
    private static final HashMap<String, Integer> vendorCollectionFreq;
    private static final List<String> vendors;
    private static final HashMap<String, Integer> vendorMaxActiveTasks;
    private static final HashMap<String, String> vendorDomainFields;
    private static final HashMap<String, String> vendorDomainCheckFields;

    static {
        int monthInSecs = 86400 * 30;
        vendorCollectionFreq = new HashMap<>();
        vendorCollectionFreq.put(VENDOR_ALEXA, monthInSecs * 3);
        vendorCollectionFreq.put(VENDOR_BUILTWITH, monthInSecs * 6);
        vendorCollectionFreq.put(VENDOR_COMPETE, monthInSecs);
        vendorCollectionFreq.put(VENDOR_FEATURE, monthInSecs * 6);
        vendorCollectionFreq.put(VENDOR_HPA_NEW, monthInSecs * 6);
        vendorCollectionFreq.put(VENDOR_ORBI_V2, monthInSecs * 3);
        vendorCollectionFreq.put(VENDOR_SEMRUSH, monthInSecs);

        List<String> temp = new ArrayList<String>(VENDOR_COUNT);
        temp.add(VENDOR_ALEXA);
        temp.add(VENDOR_BUILTWITH);
        temp.add(VENDOR_COMPETE);
        temp.add(VENDOR_FEATURE);
        temp.add(VENDOR_HPA_NEW);
        temp.add(VENDOR_ORBI_V2);
        temp.add(VENDOR_SEMRUSH);
        vendors = temp;

        vendorMaxActiveTasks = new HashMap<>();
        vendorMaxActiveTasks.put(VENDOR_ALEXA, 1);
        vendorMaxActiveTasks.put(VENDOR_BUILTWITH, 1);
        vendorMaxActiveTasks.put(VENDOR_COMPETE, 1);
        vendorMaxActiveTasks.put(VENDOR_FEATURE, 1);
        vendorMaxActiveTasks.put(VENDOR_HPA_NEW, 1);
        vendorMaxActiveTasks.put(VENDOR_ORBI_V2, 1);
        vendorMaxActiveTasks.put(VENDOR_SEMRUSH, 1);

        vendorDomainFields = new HashMap<>();
        vendorDomainFields.put(VENDOR_BUILTWITH, "Domain");

        vendorDomainCheckFields = new HashMap<>();
        vendorDomainCheckFields.put(VENDOR_BUILTWITH, "Technology_Name");
    }

    public static String getDomainField(String vendor) {
        return vendorDomainFields.get(vendor);
    }

    public static String getDomainCheckField(String vendor) {
        return vendorDomainCheckFields.get(vendor);
    }

    public static int getVendorCollectingFreq(String vendor) {
        return vendorCollectionFreq.get(vendor);
    }

    public static List<String> getVendors() {
        return vendors;
    }

    public static int getMaxActiveTasks(String vendor) {
        vendor = vendor.toUpperCase();
        if (!vendorMaxActiveTasks.containsKey(vendor))
            return 0;

        return vendorMaxActiveTasks.get(vendor);
    }
}
