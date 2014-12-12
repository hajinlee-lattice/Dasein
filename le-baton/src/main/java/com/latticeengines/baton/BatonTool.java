package com.latticeengines.baton;

import java.io.File;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.zookeeper.ZooDefs;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleConfiguration;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.CamilleEnvironment.Mode;
import com.latticeengines.camille.exposed.paths.FileSystemGetChildrenFunction;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;

import ch.qos.logback.classic.LoggerContext;

public class BatonTool {
	private static final Logger log = LoggerFactory.getLogger(new Object() {
	}.getClass().getEnclosingClass());

	static {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
				if (loggerFactory instanceof LoggerContext) {
					((LoggerContext) loggerFactory).stop();
				}
			}
		});
	}

	public static void main(String[] args) {
		ArgumentParser parser = ArgumentParsers.newArgumentParser("Baton");
		parser.addArgument("-createPod").action(Arguments.storeTrue())
				.help("Creates a Pod based on ZooKeeper.json file");

		parser.addArgument("--podID", "--pid").required(true).help("Camille PodID");
		parser.addArgument("--connectionString", "--cs").required(true).help("Connection string for ZooKeeper");

		// Don't let PLO know about this...
		parser.addArgument("-loadDirectory").action(Arguments.storeTrue()).help(Arguments.SUPPRESS);
		parser.addArgument("--source", "--S", "--s").help(Arguments.SUPPRESS);
		parser.addArgument("--destination", "--D", "--d").help(Arguments.SUPPRESS);
		parser.addArgument("--force", "--F", "--f").action(Arguments.storeTrue()).help(Arguments.SUPPRESS);

		Namespace namespace = null;
		try {
			namespace = parser.parseArgs(args);
		} catch (ArgumentParserException e) {
			log.error("Error parsing input arguments", e);
			System.exit(1);
		}

		try {
			CamilleConfiguration config = new CamilleConfiguration();
			config.setConnectionString((String) namespace.get("connectionString"));
			config.setPodId((String) namespace.get("podID"));
			CamilleEnvironment.start(Mode.BOOTSTRAP, config);

		} catch (Exception e) {
			log.error("Error starting Camille", e);
			System.exit(1);
		}

		if (namespace.get("loadDirectory")) {
			String source = namespace.get("source");
			String destination = namespace.get("destination");
			boolean force = namespace.get("force");

			if (source == null || destination == null) {
				log.error("LoadDirectory requires Source and Destination");
				System.exit(1);
			}

			else {
				loadDirectory(source, destination, force);
			}
		}

		if (namespace.get("createPod")) {
			log.info("Sucesfully created pod");
		}
	}

	/**
	 * Loads directory into ZooKeeper
	 * 
	 * @param source
	 *            Path of files to load
	 * @param destination
	 *            Path in ZooKeeper to store files
	 * @param force
	 *            Bool of whether or not to override path in ZooKeeper if it
	 *            already exists
	 */
	private static void loadDirectory(String source, String destination, boolean force) {
		try {
			Camille c = CamilleEnvironment.getCamille();
			destination = String.format("/Pods/%s/%s", CamilleEnvironment.getPodId(), destination);

			File f = new File(source);
			DocumentDirectory docDir = new DocumentDirectory(new Path("/"), new FileSystemGetChildrenFunction(f));
			Path parent = new Path(destination);

			if (c.exists(parent)) {
				if (force) {
					c.delete(parent);
				} else {
					log.error(String.format("Error: Destination %s already exists", destination));
					System.exit(1);
				}
			}
			c.createDirectory(parent, docDir, ZooDefs.Ids.OPEN_ACL_UNSAFE);

		} catch (Exception e) {
			log.error("Error loading directory", e);
			System.exit(1);
		}

		log.info("Sucesfully loaded files into directory");
	}
}
