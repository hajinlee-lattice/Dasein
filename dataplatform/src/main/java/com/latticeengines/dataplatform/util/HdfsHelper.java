package com.latticeengines.dataplatform.util;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.util.StreamUtils;

public class HdfsHelper {

	public static final String getHdfsFileContents(Configuration configuration, String hdfsPath) throws Exception {
		FileSystem fs = FileSystem.get(configuration);
		Path schemaPath = new Path(hdfsPath);
		InputStream is = fs.open(schemaPath);
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		StreamUtils.copy(is, os);
		return new String(os.toByteArray());
	}
}
