package com.voya.common;

import org.eclipse.jetty.util.log.Log;
import com.gemstone.gemfire.cache.execute.FunctionContext;

public class ExceptionHelpers {
    public static void sendStrippedException(FunctionContext context, Exception exception) {
        RuntimeException serializableException = new RuntimeException(exception.getMessage());
        serializableException.setStackTrace(exception.getStackTrace());
        Log.getLog().info(exception.getMessage());
        context.getResultSender().lastResult(exception.getMessage());
    }
}
