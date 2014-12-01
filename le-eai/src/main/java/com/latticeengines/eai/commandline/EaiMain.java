package com.latticeengines.eai.commandline;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.exposed.service.impl.DataExtractionServiceImpl;
import com.latticeengines.eai.routes.ImportProperty;

public class EaiMain implements ApplicationContextAware{

	private ApplicationContext applicationContext;

	@SuppressWarnings("unused")
	private static final Log log = LogFactory.getLog(EaiMain.class);

	public EaiMain() {
		ConfigurableApplicationContext ctx = new ClassPathXmlApplicationContext(
				"eai-context.xml");
		setApplicationContext(ctx);
	}

	public static void main(String[] args) throws IOException, ParseException {
		String s = FileUtils.readFileToString(new File(args[0]));
		JSONParser parser = new JSONParser();
		JSONArray resultArr = (JSONArray) parser.parse(s);
		List<Table> tables = new ArrayList<>();
		for(Object obj: resultArr.toArray()){
			tables.add(JsonUtils.deserialize(obj.toString(), Table.class));
		}
		EaiMain eai = new EaiMain();
		
		ImportContext context = new ImportContext();
		Configuration config = new YarnConfiguration();
		context.setProperty(ImportProperty.HADOOPCONFIG, config);
		context.setProperty(ImportProperty.TARGETPATH, args[1]);
		ApplicationContext applicationContext = eai.getApplicationContext();
		DataExtractionServiceImpl dataExtractionService = (DataExtractionServiceImpl) applicationContext.getBean("dataExtractionService");
		dataExtractionService.extractAndImport(tables, context);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.applicationContext = applicationContext;
	}
	
	public ApplicationContext getApplicationContext(){
		return this.applicationContext;
	}
}
