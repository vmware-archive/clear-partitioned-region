package io.pivotal.collectors;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;

public class MockResultCollector<T,S> implements ResultCollector<T,S> {

	public MockResultCollector() {
		super();
	}

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

	@SuppressWarnings("unchecked")
	@Override
	public S getResult() throws FunctionException {
		return (S) results;
	}

	@Override
	public S getResult(long timeout, TimeUnit unit) throws FunctionException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addResult(DistributedMember memberID, T resultOfSingleExecution) {
		// TODO Auto-generated method stub
//		System.out.println(resultOfSingleExecution.getClass().getName());
		results.add(resultOfSingleExecution);
	}

	@Override
	public void endResults() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void clearResults() {
		results.clear();
	}
}
