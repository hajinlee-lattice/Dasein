package com.latticeengines.dataplatform.util;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.util.StreamUtils;

public class HdfsHelper {

	public static final String getHdfsFileContents(Configuration configuration,
			String hdfsPath) throws Exception {
		FileSystem fs = FileSystem.get(configuration);
		Path schemaPath = new Path(hdfsPath);
		InputStream is = fs.open(schemaPath);
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		StreamUtils.copy(is, os);
		return new String(os.toByteArray());
	}

	public static final List<String> getFilesForDir(
			Configuration configuration, String hdfsDir) throws Exception {
		FileSystem fs = FileSystem.get(configuration);
		FileStatus[] statuses = fs.listStatus(new Path(hdfsDir));
		List<String> filePaths = new ArrayList<String>();
		for (FileStatus status : statuses) {
			filePaths.add(status.getPath().toString());
		}

		return filePaths;
	}
	
}
