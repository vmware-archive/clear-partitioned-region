package com.voya.service;

import static com.voya.common.ExceptionHelpers.sendStrippedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.execute.ResultSender;

public class ClearRegion {

	private Cache cache;
	private LogWriter log;
	private ResultSender<Object> resultSender;
	
	public ClearRegion(Cache cache, ResultSender<Object> resultSender) {
		this.log = cache.getLogger();
		this.cache = cache;
		this.resultSender = resultSender;
	}
	
	public void clearRegions(String[] regionNames) {
		Set<String> allRegions = new HashSet<String>();
		List<String> regionsWithRegexExpressions = new ArrayList<String>();
       	for (String regionName : regionNames) {
       		if (regionName.contains("*") || regionName.contains("?")) {
       			regionsWithRegexExpressions.add(regionName);
       		}
       		else {
       			allRegions.add(regionName);
       		}
       	}
       	if (!regionsWithRegexExpressions.isEmpty()) {
       		allRegions.addAll(findRegionsLike(regionsWithRegexExpressions));
       	}
		for (String regionName : allRegions) {
    	  clearRegion(regionName);
		}
	}
	
	private void clearRegion(String regionName) {
      try {
	   	  log.info("ClearRegion> processing region:" + regionName);
		
	   	  Region<?, ?> region = cache.getRegion(regionName);
		  
	   	  if (region == null) {
			  log.error("regionName " + regionName + " does not exist.");
		      resultSender.sendResult("regionName " + regionName + " does not exist.");
	   	  }
	   	  else {
	   		  removeEntriesFromRegion(region);
	   	  }	 
      } catch (Exception exception) {
	    sendStrippedException(resultSender, exception);
      }
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void removeEntriesFromRegion(Region<?, ?> region) {
		  log.info("Got region:" + region.getFullPath());
	      Set<?> keys = region.keySet();
	      Integer numberOfEntries = keys.size();
	 
	      log.info("Removing " + numberOfEntries + " entries from " + region.getName() + " region.");

	      // if the key is a String then shortcut the clearing process
	      RegionAttributes<?, ?> regionAttributes = region.getAttributes();
	      Class<?> clazz = regionAttributes.getKeyConstraint();
	      if (clazz != null && clazz == String.class) {
	    	region.removeAll((Collection) keys);
	      }
	      else {
	        removeOneByOne(region, keys);
	      }
	      
	      log.fine("Sending back " + numberOfEntries + " entries");
	      resultSender.sendResult("regionName " + region.getName() + ": " + numberOfEntries);
	}
	
	private void removeOneByOne(Region<?, ?> region, Set<?> keys) {
      // loop rather than removeAll because Voya may use a non-String key
      for (Object key : keys) {
        region.remove(key);
      }
	}
	
	/**
	 * Pass in an array of region names that have regex expressions.
	 * This method will loop through all region names to match against
	 * the array
	 * 
	 * @param regionsWithRegexExpressions
	 */
	public Set<String> findRegionsLike(List<String> regionsWithRegexExpressions) {
	  Set<Region<?,?>> allRegions = cache.rootRegions();
	  Set<String> matchingRegionNames = new HashSet<String>();
		
	  for (String regionName : regionsWithRegexExpressions) {
	    String regionRegexed = regionName.replace("*", ".*").replace("?", ".?");
	      for (Region<?,?> region : allRegions) {
		    if (region.getName().matches(regionRegexed)) {
			  matchingRegionNames.add(region.getName());
		    }
	      }
        }
		
	  return matchingRegionNames;
	}
}
