package io.pivotal.function;

import static io.pivotal.common.ExceptionHelpers.logException;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import io.pivotal.common.ClearRegionArgumentsParser;
import io.pivotal.domain.events.RegionClearedEvent;
import io.pivotal.service.ClearRegionParallel;

public class ClearRegionParallelFunction implements Function, Declarable {
	private static final long serialVersionUID = 1L;
	private Cache cache;
	private LogWriter log;
	private String regionNameBeingCleared = null;
	boolean isExecutionDelegatedFromAnotherNode = false;
	String delegatedExecutionInd = null;
	ClearRegionParallel clearRegionService = null;

	/**
	 * This clears a partitioned region's entries. This must execute on a
	 * partitioned region. Regions are passed in as a comma delimited string of
	 * region names.
	 * 
	 * Call this function using .onRegion with a partitioned region
	 * Call using .onServer(pool) using 1 or more replicated regions
	 *   - you can specify multiple replicated regions in a single line, like "Region1, Region2,Region3".
	 */
	public void execute(FunctionContext context) {
		try {
			cache = CacheFactory.getAnyInstance();
			log = cache.getLogger();
			Object args = context.getArguments();
			Map<String, String> parsedArgs = parseArguments(args);
			String passedRegionNames = parsedArgs.get("regionNames");

			DistributedMember thisMember = cache.getDistributedSystem().getDistributedMember();
			log.info(getId() + " is executing on member: " + thisMember.getName());
			clearRegionService = new ClearRegionParallel(cache, context.getResultSender(), thisMember.getName());
			
			String[] regionNames = parseRegionNames(passedRegionNames);
			for (String regionName : regionNames) {
				regionNameBeingCleared = regionName;
				clearRegion(regionName, thisMember, context);
			}

		} catch (Exception exception) {
			String message = logException(exception, log);
			context.getResultSender().lastResult("Exception removing from region " + regionNameBeingCleared + ", exception=" + message);
		}
	}

	/*
	 * Clears a single partitioned or replicated region
	 */
	private void clearRegion(String regionName, DistributedMember thisMember, FunctionContext context) {
		Region<?, ?> region = validateRegionToClear(regionName);
		
		if (region instanceof PartitionedRegion) {
			clearPartitionedRegion(region, context, thisMember);
			return;
		}

		/*
		 *  a replicated region
		 */

		/*
		 *  This function can delegate execution for clearing a replicated region to a different node.
		 *  When it does so, it fills in the delegatedExecutionInd
		 */
		if (delegatedExecutionInd != null && delegatedExecutionInd.equalsIgnoreCase("execution_delegated")) {
			isExecutionDelegatedFromAnotherNode = true;
		}

		// if another node delegated clearing the replicate region to this node, clear and return
		if (isExecutionDelegatedFromAnotherNode) {
			executeClearOnThisMember(region, context, thisMember);	
			return;
		}

		/*
		 *  We are here because we are a replicated region.
		 *  We determine on which node to execute the clear so as to balance execution
		 *  resources in the case of multiple sequential executions for different regions.
		 */

		DistributedMember targetMember = selectTargetedMember(thisMember, regionName);

		// if I am the targeted node, execute and return
		boolean isTargetedMemberThisMember = thisMember.getName().equalsIgnoreCase(targetMember.getName());
		if (isTargetedMemberThisMember) {
			executeClearOnThisMember(region, context, thisMember);
			return;
		}
		
		// delegate clearing of the replicated region to a different node
		executeOnTargetedNode(targetMember, regionName, context);
	}
	
	private String[] parseRegionNames(String passedRegionNames) {
		String[] regionNames;
		if (!passedRegionNames.contains(",")) {
			regionNames = new String[1];
			regionNames[0] = passedRegionNames;
		}
		else {
			regionNames = passedRegionNames.split(",");
		}
		return regionNames;
	}
	
	/*
	 * Execute this clear operation on a different targeted member
	 */
	private void executeOnTargetedNode(DistributedMember targetMember, String regionName, FunctionContext context) {
		log.info("Transferring clear region execution for " + regionName + " to member: " + targetMember.getName());
		String[] arguments = new String[2];
		arguments[0] = regionName;
		arguments[1] = "execution_delegated";
		Execution execution = FunctionService.onMember(targetMember).withArgs(arguments);
		@SuppressWarnings("unchecked")
		ResultCollector<String, List<String>> rc = (ResultCollector<String, List<String>>) execution
				.execute(getId());
		List<String> results = rc.getResult();
		int messageIx = 0;
		for (messageIx = 0; messageIx < results.size(); messageIx++) {
			log.fine("result " + messageIx + " from " + targetMember + ": " + results.get(messageIx));
			if (results.get(messageIx) == null || results.get(messageIx).length() == 0) {
				continue;
			}
		}
		String result = results.get(messageIx - 1);
		context.getResultSender().lastResult(result);
	}
	
	/*
	 * The partitioned region will only clear entries where the primaries exist on this member
	 * and the corresponding backups on other members for those primaries.
	 */
	private void clearPartitionedRegion(Region<?, ?> region, FunctionContext context, DistributedMember thisMember) {
		if (!(context instanceof RegionFunctionContext)) {
			throw new FunctionException(
					"This is a data aware function, and has to be called using FunctionService.onRegion.");
		}
		RegionFunctionContext rfc = (RegionFunctionContext) context;
		region = PartitionRegionHelper.getLocalDataForContext(rfc);
		executeClearOnThisMember(region, context, thisMember);
	}

	private void executeClearOnThisMember(Region<?, ?> region, FunctionContext context, DistributedMember member) {
		RegionClearedEvent event = clearRegionService.clearRegion(region);
		context.getResultSender().lastResult("Removed from region " + event.regionName() + ", member=" + event.memberName() +": " + event.numberOfEntriesRemoved());
	}
	
	/*
	 * Selects a random node on which to execute processing so as to balance resources
	 */
	private DistributedMember selectTargetedMember(DistributedMember thisMember, String regionName) {
		DistributedMember targetedMember = clearRegionService.selectTargetedMember(thisMember, regionName);
		log.info("Selecting target member=" + targetedMember.getName());
		return targetedMember;
	}

	private Map<String, String> parseArguments(Object args) {
		ClearRegionArgumentsParser argsParser = new ClearRegionArgumentsParser(args);
		Map<String, String> parsedArgs = argsParser.parseArguments();
		return parsedArgs;
	}
	
	private Region<?,?> validateRegionToClear(String regionName) {
		Region<?, ?> region = cache.getRegion(regionName);
		if (region == null) {
			throw new RuntimeException("Region does not exist: " + regionName);
		}
		return region;
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.gemstone.gemfire.cache.execute.Function#getId()
	 */
	public String getId() {
		return getClass().getSimpleName();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.gemstone.gemfire.cache.execute.Function#optimizeForWrite()
	 */
	public boolean optimizeForWrite() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.gemstone.gemfire.cache.execute.Function#isHA()
	 */
	public boolean isHA() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.gemstone.gemfire.cache.execute.Function#hasResult()
	 */
	public boolean hasResult() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.gemstone.gemfire.cache.Declarable#init(java.util.Properties)
	 */
	public void init(final Properties properties) {
	}
}
