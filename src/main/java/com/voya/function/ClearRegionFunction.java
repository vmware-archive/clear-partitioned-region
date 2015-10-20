package com.voya.function;

import static com.voya.common.ExceptionHelpers.sendStrippedException;

import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.voya.service.ClearRegion;

public class ClearRegionFunction implements Function, Declarable {
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
        	
        	ClearRegion clearRegion = new ClearRegion(cache, context.getResultSender());
        	clearRegion.clearRegions(regionNames);
       	} catch (Exception exception) {
    		sendStrippedException(context.getResultSender(), exception);
    	}
        context.getResultSender().lastResult("");
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
