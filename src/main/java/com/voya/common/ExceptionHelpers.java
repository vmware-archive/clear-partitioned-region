package com.voya.common;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.execute.ResultSender;

public class ExceptionHelpers {
    public static void sendStrippedException(ResultSender<Object> resultSender, Exception e, LogWriter log) {
        RuntimeException serializableException = new RuntimeException(e.getMessage());
        serializableException.setStackTrace(e.getStackTrace());
        log.error(e);
        resultSender.lastResult(e.getMessage());
    }
}
