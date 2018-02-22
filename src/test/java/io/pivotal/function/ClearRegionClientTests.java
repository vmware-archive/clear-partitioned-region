package io.pivotal.function;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;

import io.pivotal.collectors.MockResultCollector;
import io.pivotal.collectors.MockResultSender;

/**
 * Unit test for simple App.
 */
public class ClearRegionClientTests

{
	private static ClientCache clientCache;
	private static MockResultSender<String> resultSender;
	private static ResultCollector<Object, Object> resultCollector = new MockResultCollector<Object, Object>();
	private static Pool pool;
	private static Region pRegion = null;
	private static Region<?, ?> rRegion = null;
	Random r = new Random(new Date().getTime());
	DateFormat df = new SimpleDateFormat("HH:mm:ss.SSS");

	@BeforeClass
	public static void init() {
		clientCache = new ClientCacheFactory().set("name", "RegionCleaner").set("log-level", "error").create();
		PoolFactory pf = PoolManager.createFactory();
		pf.addLocator("localhost", 10334);
		pool = pf.create("pool");
		pRegion = clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).setPoolName("pool")
				.create("pRegion");
		rRegion = clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).setPoolName("pool")
				.create("rRegion");
		resultSender = new MockResultSender<String>();

	}

	/**
	 * Test clearing regions using regex. Create a few regions beginning with
	 * Test...
	 */
	@Test
	public void testClearMultipleReplicatedRegions() {
		System.out.println("Multiple Replicated Regions - Partitioned regions should fail");

		String regionNamesString = "Test1,Test2,Test3,Test4,Test5,Test6";
		String[] regionNames = regionNamesString.split(",");
		for (String regionName : regionNames) {
			long startTime = System.currentTimeMillis();
			ResultCollector<String, List<String>> rc = callFunctionInParallelForReplicated(regionName, true);
			List<String> result = rc.getResult();
			for (int j = 0; j < result.size(); j++) {
				if (result.get(j).trim().length() > 0) {
					System.out.println(df.format(new Date()) + " - " + j + ": " + result.get(j) + ", elapsed=" + (System.currentTimeMillis() - startTime));
				}
			}
			rc.clearResults();
		}
		System.out.println("***************************************************************");
	}

	/**
	 * Test clearing regions using regex. Create a few regions beginning with
	 * Test...
	 */
	@Test
	public void testClearMultiplePartitionedRegions() {
		System.out.println("Multiple Partitioned Regions - All should work");
		String regionNamesString = "Test1,Test2,Test3,Test4,Test5,Test6";
		String[] regionNames = regionNamesString.split(",");
		for (String regionName : regionNames) {
			long startTime = System.currentTimeMillis();
			ResultCollector<String, List<String>> rc = callFunctionInParallel(regionName, true);
			List<String> result = rc.getResult();
			for (int j = 0; j < result.size(); j++) {
				if (result.get(j).trim().length() > 0) {
					System.out.println(df.format(new Date()) + " - " + j + ": " + result.get(j)  + ", elapsed=" + (System.currentTimeMillis() - startTime));
				}
			}
			rc.clearResults();
		}
		System.out.println("***************************************************************");
	}

	/**
	 * Test clearing regions without using regex
	 */
	@Test
	public void testClearRegionInParallel() {
		String regionName = "pRegion";
		for (int i = 0; i < 10; i++) {
			long startTime = System.currentTimeMillis();
			ResultCollector<String, List<String>> rc = callFunctionInParallel(regionName, true);
			List<String> result = rc.getResult();
			for (int j = 0; j < result.size(); j++) {
				if (result.get(j).trim().length() > 0) {
					System.out.println(df.format(new Date()) + " " + i + " - " + j + ": " + result.get(j) + ", elapsed=" + (System.currentTimeMillis() - startTime));
				}
			}
			rc.clearResults();
		}
		System.out.println("***************************************************************");

		for (int i = 0; i < 10; i++) {
			regionName = "rRegion";
			long startTime = System.currentTimeMillis();
			ResultCollector<String, List<String>> rc = callFunctionInParallel(regionName, true);
			List<String> result = rc.getResult();
			for (int j = 0; j < result.size(); j++) {
				if (result.get(j).trim().length() > 0) {
					System.out.println(df.format(new Date()) + " " + i + " - " + j + ": " + result.get(j) + ", elapsed=" + (System.currentTimeMillis() - startTime));
				}
			}
			rc.clearResults();
		}
		System.out.println("***************************************************************");
	}

	private void callFunction(String[] regionNames) {
		Execution execution = FunctionService.onServer(pool).withArgs("foo").withCollector(resultCollector);
		execution.execute("ClearRegionFunction");
	}

	private int generateRandomEntries(Region<String, String> region) {
		int numberOfEntries = generateRandomNumberOfEntries(region);
		System.out.println("\n" + df.format(new Date()) + ": Request to delete " + numberOfEntries + " entries");
		return numberOfEntries;
	}

	private ResultCollector<String, List<String>> callFunctionInParallel(String regionName,
			boolean doGenerateRandomEntries) {
		Region region = clientCache.getRegion(regionName);
		if (region == null) {
			region = clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).setPoolName("pool")
					.create(regionName);

		}
		if (doGenerateRandomEntries) {
			generateRandomEntries(region);
		}
		// Set<?> keys = region.keySetOnServer();
		// Execution execution =
		// FunctionService.onRegion(region).withFilter(keys).withArgs(regionName)
		// .withCollector(resultCollector);
		Execution execution = FunctionService.onRegion(region).withArgs(regionName).withCollector(resultCollector);
		@SuppressWarnings("unchecked")
		ResultCollector<String, List<String>> rc = (ResultCollector<String, List<String>>) execution
				.execute("ClearRegionParallelFunction");
		return rc;
	}

	private ResultCollector<String, List<String>> callFunctionInParallelForReplicated(String regionNameString,
			boolean doGenerateRandomEntries) {

		String[] regionNames;
		if (regionNameString.contains(",")) {
			regionNames = new String[1];
			regionNames[0] = regionNameString;
		} else {
			regionNames = regionNameString.split(",");
		}

		for (String regionName : regionNames) {
			Region region = clientCache.getRegion(regionName);
			if (region == null) {
				region = clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).setPoolName("pool")
						.create(regionName);

			}
			if (doGenerateRandomEntries) {
				generateRandomEntries(region);
			}
		}
		// Set<?> keys = region.keySetOnServer();
		// Execution execution =
		// FunctionService.onRegion(region).withFilter(keys).withArgs(regionName)
		// .withCollector(resultCollector);
		Execution execution = FunctionService.onServer(pool).withArgs(regionNameString).withCollector(resultCollector);
		@SuppressWarnings("unchecked")
		ResultCollector<String, List<String>> rc = (ResultCollector<String, List<String>>) execution
				.execute("ClearRegionParallelFunction");
		return rc;
	}

	private int generateRandomNumberOfEntries(Region<String, String> region) {
		int numberOfEntries = 10000; //r.nextInt(1500);
		for (int i = 0; i < numberOfEntries; i++) {
			region.put(String.valueOf(i), String.valueOf(i));
		}
		return numberOfEntries;
	}
}
