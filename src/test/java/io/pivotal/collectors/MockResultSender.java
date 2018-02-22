package io.pivotal.collectors;

import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.cache.execute.ResultSender;

public class MockResultSender<T> implements ResultSender<T> {

	private List<T> results = new ArrayList<T>();
	private List<Throwable> exceptions = new ArrayList<Throwable>();
	
	public void sendResult(T oneResult) {
		results.add(oneResult);
	}

	public void lastResult(T lastResult) {
		results.add(lastResult);
	}

	public void sendException(Throwable t) {
		exceptions.add(t);
	}

	public List<T> getResults() {
		return results;
	}

	public List<Throwable> getExceptions() {
		return exceptions;
	}
}
