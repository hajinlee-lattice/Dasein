package com.latticeengines.camille;

import com.latticeengines.logging.DefaultAppender;
import com.latticeengines.logging.LoggerAdapter;

public class Wrapper {
    public static void main(String[] args) throws Exception {
        LoggerAdapter.addAppender(new DefaultAppender(System.out));
        App.main(new String[] { "-connectionString", "127.0.0.1:2181" });
        System.exit(0);
    }
}
