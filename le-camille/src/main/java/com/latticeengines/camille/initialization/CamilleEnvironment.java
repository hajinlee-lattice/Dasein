package com.latticeengines.camille.initialization;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class CamilleEnvironment {

	// singleton instance
	private static CuratorFramework client;
	
    // these are reasonable arguments for the ExponentialBackoffRetry. The first
    // retry will wait 1 second - the second will wait up to 2 seconds - the
    // third will wait up to 4 seconds.
    private static ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
	
	private static Initializer init;
	
	public static void Initalize(Initializer init) throws InterruptedException {
		CamilleEnvironment.init = init;
		
		start();
	}
	
	// TODO: make this public ?
	private static void start() throws InterruptedException {
		if (client == null || !client.getState().equals(CuratorFrameworkState.STARTED)) {
			client = CuratorFrameworkFactory.newClient(init.getConnectionString(), retryPolicy);
			client.start();
			client.blockUntilConnected();
		}
		
		if (init instanceof Bootstrap) {
			// TODO: do whatever extra tasks Bootstrapping requires such as creating the Pods directory, etc.
		}
	}
	
	public static void stop() {
		client.close();
	}
	
	public static CuratorFramework getCuratorClient() {
		return client;
	}
}
