package io.pivotal.domain.events;

import java.util.Date;

public class RegionClearedEvent implements DomainEvent {

	private String regionName;
	private String memberName;
	private int numberOfEntriesRemoved;
	private int eventVersion;
	private Date occurredOn;
	private String regionType;

	public RegionClearedEvent(String regionName, String memberName, int numberOfEntriesRemoved, String regionType) {
		super();
		this.regionName = regionName;
		this.memberName = memberName;
		this.numberOfEntriesRemoved = numberOfEntriesRemoved;
		this.eventVersion = 1;
		this.occurredOn = new Date();
		this.regionType = regionType;
	}

	@Override
	public int eventVersion() {
		return this.eventVersion;
	}

	@Override
	public Date occurredOn() {
		return this.occurredOn;
	}

	public String regionName() {
		return regionName;
	}

	public String memberName() {
		return memberName;
	}

	public int numberOfEntriesRemoved() {
		return numberOfEntriesRemoved;
	}

	public int getEventVersion() {
		return eventVersion;
	}

	public Date getOccurredOn() {
		return occurredOn;
	}

	public String regionType() {
		return regionType;
	}

	public void setRegionName(String regionName) {
		this.regionName = regionName;
	}

	public void setMemberName(String memberName) {
		this.memberName = memberName;
	}

	public void setNumberOfEntriesRemoved(int numberOfEntriesRemoved) {
		this.numberOfEntriesRemoved = numberOfEntriesRemoved;
	}

	public void setEventVersion(int eventVersion) {
		this.eventVersion = eventVersion;
	}

	public void setOccurredOn(Date occurredOn) {
		this.occurredOn = occurredOn;
	}

	public void setRegionType(String regionType) {
		this.regionType = regionType;
	}

}
