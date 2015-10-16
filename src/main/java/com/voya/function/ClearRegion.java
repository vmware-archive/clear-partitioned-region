package com.voya.function;

import static com.voya.common.ExceptionHelpers.sendStrippedException;

import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;

public class ClearRegion implements Function, Declarable {
  private static final long serialVersionUID = 1L;
  private Cache cache;
    /**
     * This clears a partitioned region's entries.
     * This must execute on a partitioned region.
     * Regions are passed in as a comma delimited string of region names
     */
	@SuppressWarnings("unchecked")
	public void execute(FunctionContext context) {
        try {
        	cache = CacheFactory.getAnyInstance();
        	Object argument = context.getArguments();
        	
         	// when passing from a client, this is a String.
        	// when executing from gfsh, it is a String[]
        	String regionNames[] = null;
        	if (argument instanceof String) {
        		regionNames = ((String) argument).split(",");
        	}
        	else if (argument instanceof String[]) {
        		regionNames = (String[]) argument;
        	}
        	else if (argument instanceof List) {
        		regionNames = ((List<String[]>) argument).get(0);
        	}
        	else {
        		throw new RuntimeException("I do not understand the type of region name passed to me: " + argument.getClass().getName());
        	}
        	
        	for (String regionName : regionNames) {
        		clearRegion(regionName, context.getResultSender());
        	}
       	} catch (Exception exception) {
    		sendStrippedException(context.getResultSender(), exception);
    	}
        context.getResultSender().lastResult("");
     }

	private void clearRegion(String regionName, ResultSender<Object> resultSender) {
		try {
       	  cache.getLogger().info("ClearRegion> processing region:" + regionName);
    	
       	  Region<?, ?> region = cache.getRegion(regionName);
		    
		  cache.getLogger().fine("I'm past getting the region");
		  cache.getLogger().info("Got region:" + region.getFullPath());
             
          Set<?> keys = region.keySet();
          Integer numberOfEntries = keys.size();
 
          cache.getLogger()
              .info("Removing " + numberOfEntries + " entries from " + region.getName() + " region.");

          // loop rather than removeAll because Voya may use a non-String key
          for (Object key : keys) {
            region.remove(key);
          }
 
          cache.getLogger()
             .fine("Sending back " + numberOfEntries + " entries");

          // return the number of deletes or an error message
          resultSender.sendResult(numberOfEntries);

    	} catch (Exception exception) {
    		sendStrippedException(resultSender, exception);
    	}
    }
	
    /*
     * (non-Javadoc)
     * @see com.gemstone.gemfire.cache.execute.Function#getId()
     */
    public String getId() {
        return getClass().getSimpleName();
    }

    /*
     * (non-Javadoc)
     * @see com.gemstone.gemfire.cache.execute.Function#optimizeForWrite()
     */
     public boolean optimizeForWrite() {
        return false;
    }

    /*
     * (non-Javadoc)
     * @see com.gemstone.gemfire.cache.execute.Function#isHA()
     */
    public boolean isHA() {
        return true;
    }

    /*
     * (non-Javadoc)
     * @see com.gemstone.gemfire.cache.execute.Function#hasResult()
     */
    public boolean hasResult() {
      return true;
    }

    /*
     * (non-Javadoc)
     * @see com.gemstone.gemfire.cache.Declarable#init(java.util.Properties)
     */
    public void init(final Properties properties) {
    }
}
