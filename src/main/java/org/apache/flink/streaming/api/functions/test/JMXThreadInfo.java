package org.apache.flink.streaming.api.functions.test;

/**
 * Created by zhangjianxin on 2017/11/30.
 * Github Breakeval13
 * blog firsh.me
 */
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.EnumMap;
import java.util.Map;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public class JMXThreadInfo {

    public static String MXBEAN_NAME = ManagementFactory.THREAD_MXBEAN_NAME;

    ObjectName threadsName;
    int numThreadStates = Thread.State.values().length;
    EnumMap<Thread.State, Long> initialThreadStates = new EnumMap<Thread.State, Long>(Thread.State.class);

    public JMXThreadInfo() {
        try {
            threadsName = new ObjectName(MXBEAN_NAME);
        } catch (MalformedObjectNameException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        }

        for(Thread.State state : Thread.State.values()) {
            initialThreadStates.put(state, new Long(0));
        }

    }

    public Map<Thread.State, Long> getCurrentInfo(MBeanServerConnection connection) {

        ThreadMXBean threadBean = JMX.newMXBeanProxy(connection, threadsName, ThreadMXBean.class);
        ThreadInfo[] threads = threadBean.dumpAllThreads(false,false);

        // take copy of map with all possible states initialised to zero
        EnumMap<Thread.State, Long> threadStates = initialThreadStates.clone();

        for(ThreadInfo thread : threads) {
            Thread.State state = thread.getThreadState();
            threadStates.put(state, threadStates.get(state)+1);
        }

        return threadStates;

    }

}