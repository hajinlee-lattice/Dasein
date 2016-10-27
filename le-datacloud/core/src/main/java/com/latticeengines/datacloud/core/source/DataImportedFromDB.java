package com.latticeengines.datacloud.core.source;

public interface DataImportedFromDB extends IngestedRawSource {
	/*
	 * used for sqoop import
	 */
	String getDownloadSplitColumn();

}
