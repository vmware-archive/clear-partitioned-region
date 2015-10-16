package com.voya.common;

import org.eclipse.jetty.util.log.Log;

import com.gemstone.gemfire.cache.execute.ResultSender;

public class ExceptionHelpers {
    public static void sendStrippedException(ResultSender<Object> resultSender, Exception exception) {
        RuntimeException serializableException = new RuntimeException(exception.getMessage());
        serializableException.setStackTrace(exception.getStackTrace());
        Log.getLog().info(exception.getMessage());
        resultSender.sendResult(exception.getMessage());
    }
}
