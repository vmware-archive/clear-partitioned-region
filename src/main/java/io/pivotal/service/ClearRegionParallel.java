package io.pivotal.service;

import static io.pivotal.common.ExceptionHelpers.logException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import io.pivotal.domain.events.RegionClearedEvent;

public class ClearRegionParallel {

	private LogWriter log;
	private String memberName;
	private Cache cache;
	Random r = new Random(new Date().getTime());

	public ClearRegionParallel(Cache cache, ResultSender<Object> resultSender, String memberName) {
		this.log = cache.getLogger();
		this.memberName = memberName;
		this.cache = cache;
	}

	public RegionClearedEvent clearRegion(Region<?, ?> region) {
		RegionClearedEvent event = null;
		try {
			Object[] keys = region.keySet().toArray();
			event = removeEntriesFromRegion(region, keys);
		} catch (Exception exception) {
			String message = logException(exception, log);
			event = new RegionClearedEvent(region.getName(), memberName, 0, message);
		}
		return event;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private RegionClearedEvent removeEntriesFromRegion(Region<?, ?> region, Object[] keys) {
		// make a copy of the keys to prevent concurrent modification exceptions
		Integer numberOfEntries = keys.length;

		log.fine("Removing " + numberOfEntries + " entries from " + region.getName() + " region.");

		// if the key is a String then shortcut the clearing process
		// remove in batches of 1,000 to relieve CPU resources on CPU starved systems
			int numberOfBatches = numberOfEntries / 1000 + 1;
			for (int i = 0; i < numberOfBatches; i++) {
				Set<Object> keyBatch = new HashSet<>();
				for (int j = 0; j < 1000; j++) {
					int k = i * 1000 + j;
					if (k >= numberOfEntries) {
						break;
					}
					keyBatch.add(keys[k]);
				}
				log.fine("About to remove " + keyBatch.size() + " entries from " + region.getName() + " region.");
				region.removeAll((Collection) keyBatch);
			}

		log.fine("Removed " + numberOfEntries + " entries");
		boolean isPartitionedRegion = region instanceof PartitionedRegion;
		RegionClearedEvent event = new RegionClearedEvent(region.getName(), memberName, numberOfEntries, isPartitionedRegion ? "Partitioned" : "Replicated");
		
		return event;
	}

	public List<DistributedMember> serverMembersInCluster(DistributedMember thisMember) {
		List<DistributedMember> serverMembers = new ArrayList<>();
		serverMembers.add(thisMember);
		log.fine("Me:  " + thisMember.getName() + ", id=" + thisMember.getId());

		Set<DistributedMember> members = cache.getDistributedSystem().getAllOtherMembers();
		// running on a single server node. Execute here
		if (members == null) {
			log.warning("I am running on a single server in the cluster");
			return serverMembers;
		}

		Iterator<DistributedMember> it = members.iterator();
		// DistributedMember[] otherMembers = (DistributedMember[]) members.toArray();
		while (it.hasNext()) {
			DistributedMember serverMember = it.next();
			String memberId = serverMember.getId();
			if (memberId.contains(":locator")) {
				continue;
			}
			serverMembers.add(serverMember);
		}

		if (log.fineEnabled()) {
			for (int i = 0; i < serverMembers.size(); i++) {
				log.fine("Available server member: " + i + ": " + serverMembers.get(i));
			}
		}

		return serverMembers;
	}

	public DistributedMember selectTargetedMember(DistributedMember thisMember, String regionName) {
		List<DistributedMember> clusterMembers = serverMembersInCluster(thisMember);

		int hash = regionName.hashCode();
		// ****************************
		// TEST ONLY!!!!!!!
		hash += r.nextInt(10);
		// ****************************

		int modula = clusterMembers.size();
		int targetNodeNumber = hash % modula;
		log.info("Selecting target node=" + targetNodeNumber + " based on modula=" + modula + " and hash=" + hash);
		return clusterMembers.get(targetNodeNumber);
	}
}
