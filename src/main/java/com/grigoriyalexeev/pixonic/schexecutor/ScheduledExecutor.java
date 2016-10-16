package com.grigoriyalexeev.pixonic.schexecutor;

import org.joda.time.DateTime;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public interface ScheduledExecutor {
    Future execute(Callable task, DateTime dateTime);
}
