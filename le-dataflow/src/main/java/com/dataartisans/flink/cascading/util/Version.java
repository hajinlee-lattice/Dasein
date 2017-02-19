/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink.cascading.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import cascading.flow.planner.PlatformInfo;

/**
 * Util class for version related information.
 */
public class Version{

	private static final String PROPERTIES_FILE = "cascading/flink/version.properties";

	private static Properties properties;

	static {
			try {
				properties = loadVersionProperties();
			}
			catch(IOException exception){
				// ignore
			}
		}

	public static String getPlatformVersion(){
		return properties.getProperty("platform.version");
	}

	public static String getPlatformName(){
		return properties.getProperty("platform.name");
	}

	public static String getPlatformVendor(){
		return properties.getProperty("platform.vendor");
	}

	public static PlatformInfo getPlatformInfo(){
		return new PlatformInfo(getPlatformName(), getPlatformVendor(), getPlatformVersion());
	}

	public static Properties loadVersionProperties() throws IOException {
		Properties properties = new Properties();
		InputStream stream = Version.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE);

		if(stream == null){
			return properties;
		}

		try{
			properties.load(stream);
		}
		finally {
			stream.close();
		}

		return properties;
	}

}
