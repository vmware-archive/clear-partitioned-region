package io.pivotal.function;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;

import io.pivotal.collectors.MockResultSender;
import io.pivotal.service.ClearRegion;

/**
 * Unit test for simple App.
 */
public class ClearRegionTests

{
	private static Cache cache;
	private static MockResultSender<Object> resultSender;
	private static ClearRegion clearRegion;

	@BeforeClass
	public static void init() {
		CacheFactory cf = new CacheFactory();
		cf.set("cache-xml-file", "./grid/config/cache.xml");
		cf.set("locators", "localhost[10334]");

		cache = cf.create();
		resultSender = new MockResultSender<Object>();
		clearRegion = new ClearRegion(cache, resultSender);
	}

	/**
	 * Test clearing regions without using regex
	 */
	@Test
	public void testClearRegion() {
		String regions = "Test6,Test42,TestWes";
		String[] regionNames = regions.split(",");
		clearRegion.clearRegions(regionNames);

		Assert.assertTrue(
				resultSender.getResults().size() == regionNames.length && resultSender.getExceptions().isEmpty());
	}

	@Test
	public void testFindRegionsWithAllRegex() {
		String regions = "Test*";
		String[] regionNames = regions.split(",");
		List<String> regionNamesList = new ArrayList<String>();
		for (String regionName : regionNames) {
			regionNamesList.add(regionName);
		}
		Set<String> matchingRegionNames = clearRegion.findRegionsLike(regionNamesList);
		Assert.assertTrue(!matchingRegionNames.isEmpty());
	}

	@Test
	public void testFindRegionsWithSomeRegex() {
		String regions = "Test??8";
		String[] regionNames = regions.split(",");
		List<String> regionNamesList = new ArrayList<String>();
		for (String regionName : regionNames) {
			regionNamesList.add(regionName);
		}
		Set<String> matchingRegionNames = clearRegion.findRegionsLike(regionNamesList);
		Assert.assertTrue(!matchingRegionNames.isEmpty());
	}

	@Test
	public void testFindRegionsWithInsaneRegex() {
		String regions = "Te?t*";
		String[] regionNames = regions.split(",");
		List<String> regionNamesList = new ArrayList<String>();
		for (String regionName : regionNames) {
			regionNamesList.add(regionName);
		}
		Set<String> matchingRegionNames = clearRegion.findRegionsLike(regionNamesList);
		Assert.assertTrue(!matchingRegionNames.isEmpty());
	}

	@Test
	public void testFindRegionsWithMultipleRegex() {
		String regions = "Test1*,Test2*";
		String[] regionNames = regions.split(",");
		List<String> regionNamesList = new ArrayList<String>();
		for (String regionName : regionNames) {
			regionNamesList.add(regionName);
		}
		Set<String> matchingRegionNames = clearRegion.findRegionsLike(regionNamesList);
		Assert.assertTrue(!matchingRegionNames.isEmpty());
	}

	@Test
	public void testFindRegionsWithBadRegion() {
		String regions = "xyz";
		String[] regionNames = regions.split(",");
		List<String> regionNamesList = new ArrayList<String>();
		for (String regionName : regionNames) {
			regionNamesList.add(regionName);
		}
		Set<String> matchingRegionNames = clearRegion.findRegionsLike(regionNamesList);
		Assert.assertTrue(matchingRegionNames.isEmpty());
	}

	@Test
	public void testClearRegionsWithMultipleRegex() {
		String regions = "Test1*,Test2*";
		String[] regionNames = regions.split(",");
		clearRegion.clearRegions(regionNames);
		Assert.assertTrue(!resultSender.getResults().isEmpty());
	}

	@Test
	public void testClearRegionsWithBadRegion() {
		String regions = "xyz";
		String[] regionNames = regions.split(",");
		clearRegion.clearRegions(regionNames);
		Assert.assertTrue(!resultSender.getResults().isEmpty());
		Assert.assertFalse(resultSender.getExceptions().isEmpty());
	}
}
