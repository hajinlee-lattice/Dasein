package com.latticeengines.camille;

import com.latticeengines.logging.DefaultAppender;
import com.latticeengines.logging.LoggerAdapter;

public class Wrapper {
    public static void main(String[] args) {
        LoggerAdapter.addAppender(new DefaultAppender(System.out));
        App.main(new String[] { "-name", "Lattice" });
        System.exit(0);
    }
}
