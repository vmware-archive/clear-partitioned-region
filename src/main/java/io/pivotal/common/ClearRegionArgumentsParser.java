package io.pivotal.common;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;

public class ClearRegionArgumentsParser {

	private Object args;
	
	public ClearRegionArgumentsParser(Object args) {
		this.args = args;
	}
	
	public Map<String, String> parseArguments() {
		// when passing from a client, args is a String.
		// when executing from gfsh, it is a String[]
		Map<String, String> parsedArguments = new HashMap<>();
		String regionNames = null;
		String delegatedExecutionInd = null;
		if (args instanceof String) {
			regionNames = (String) args;
		} else if (args instanceof String[]) {
			String[] arguments = (String[]) args;
			regionNames = arguments[0];
			if (arguments.length > 1) {
				delegatedExecutionInd = arguments[1];
			}
		} else if (args instanceof List) {
			@SuppressWarnings("unchecked")
			List<String> arguments = (List<String>) args;
			regionNames = arguments.get(0);
			if (arguments.size() > 1) {
				delegatedExecutionInd = arguments.get(1);
			}
		} else {
			throw new RuntimeException(
					"I do not understand the type of region name passed to me: " + args.getClass().getName());
		}
		parsedArguments.put("regionNames", regionNames);
		parsedArguments.put("delegatedExecutionInd", delegatedExecutionInd);
		
		return parsedArguments;
	}

	/**
	 * Pass in an array of region names that have regex expressions. This method
	 * will loop through all region names to match against the array
	 * 
	 * @param regionsWithRegexExpressions
	 */
	public String[] findRegionsLike(Cache cache, List<String> regionsWithRegexExpressions) {
		Set<Region<?, ?>> allRegions = cache.rootRegions();
		Set<String> matchingRegionNames = new HashSet<>();

		for (String regionName : regionsWithRegexExpressions) {
			String regionRegexed = regionName.replace("*", ".*").replace("?", ".?");
			for (Region<?, ?> region : allRegions) {
				if (region.getName().matches(regionRegexed)) {
					matchingRegionNames.add(region.getName());
				}
			}
		}

		return (String[]) matchingRegionNames.toArray();
	}
}
