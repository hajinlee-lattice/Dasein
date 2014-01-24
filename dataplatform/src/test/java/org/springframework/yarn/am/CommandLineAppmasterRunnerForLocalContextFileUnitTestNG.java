package org.springframework.yarn.am;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class CommandLineAppmasterRunnerForLocalContextFileUnitTestNG {

	@SuppressWarnings("unchecked")
	private static void setEnv(Map<String, String> newenv) throws Exception {
	    Class<?>[] classes = Collections.class.getDeclaredClasses();
	    Map<String, String> env = System.getenv();
	    for(Class<?> cl : classes) {
	        if("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
	            Field field = cl.getDeclaredField("m");
	            field.setAccessible(true);
	            Object obj = field.get(env);
	            Map<String, String> map = (Map<String, String>) obj;
	            map.clear();
	            map.putAll(newenv);
	        }
	    }
	}
	
	@Test(groups="functional")
	public void doMain() throws Exception {
		Map<String, String> oldEnv = System.getenv();
		Map<String, String> newEnv = new HashMap<String, String>();
		newEnv.put("CONTAINER_ID", "container_1390348594555_0043_01_000002");
		
		for (Map.Entry<String, String> entry : oldEnv.entrySet()) {
			newEnv.put(entry.getKey(), entry.getValue());
		}
		setEnv(newEnv);
		URL url = ClassLoader.getSystemResource("default/dataplatform-default-appmaster-context.xml");
		String path = url.toURI().getPath();
		CommandLineAppmasterRunnerForLocalContextFile.main(new String[] {
				path, //
				"yarnAppmaster", //
				"virtualCores=1", //
				"memory=64", //
				"priority=0", //
				"1><LOG_DIR>Appmaster.stdout",
				"2><LOG_DIR>/Appmaster.stderr"
		});
	}
}
