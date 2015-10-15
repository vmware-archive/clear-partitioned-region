package com.voya.function;

import static com.voya.common.ExceptionHelpers.sendStrippedException;

import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;

public class ClearRegion implements Function, Declarable {
  private static final long serialVersionUID = 1L;

    /**
     * This clears a partitioned region's entries.
     * This must execute on a partitioned region.
     */
	public void execute(FunctionContext context) {
        try {
        	// cast the context to a context specifically for partitioned regoins
 //   		RegionFunctionContext functionContext = (RegionFunctionContext) context;

        	Cache cache = CacheFactory.getAnyInstance();
        	cache.getLogger().fine("Im in");

        	String regionName = (String) context.getArguments();
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
            context.getResultSender().lastResult(numberOfEntries);

        } catch (Exception exception) {
            sendStrippedException(context, exception);
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
