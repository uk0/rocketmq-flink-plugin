package org.apache.flink.streaming.api.functions.test;

/**
 * Created by zhangjianxin on 2017/11/30.
 * Github Breakeval13
 * blog firsh.me
 */
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

public class JMXGarbageCollectorNotification implements Iterator<Notification> {

    private final BlockingQueue<Notification> gcNotifications  = new LinkedBlockingQueue<Notification>();


    public JMXGarbageCollectorNotification(MBeanServerConnection connection)
            throws MalformedObjectNameException, IOException, InstanceNotFoundException {

        Set<ObjectName> gcnames = connection.queryNames(new ObjectName(ManagementFactory.GARBAGE_COLLECTOR_MXBEAN_DOMAIN_TYPE + ",name=*"), null);
        HashMap<String, ObjectName> gcObjectNames = new HashMap<String, ObjectName>(gcnames.size());
        for(ObjectName mbeanName : gcnames) {
            System.err.printf("Found GC MBean %s\n", mbeanName.getCanonicalName());
            gcObjectNames.put(mbeanName.getCanonicalName(), mbeanName);
        }

        NotificationListener listener = new NotificationListener() {
            @Override
            public void handleNotification(Notification notification, Object handback) {
                gcNotifications.add(notification);
            }
        };

        for (ObjectName mbeanName : gcnames) {
            System.err.printf("Add Listener for %s\n", mbeanName.getCanonicalName());
            connection.addNotificationListener(mbeanName, listener, null, null);
        }

    }

    private Notification nextNotification;

    @Override
    public boolean hasNext() {
        if (nextNotification == null) {
            nextNotification = gcNotifications.poll();
        }
        return (nextNotification != null);

    }

    /**
     * Returns true if the iteration has more elements, waiting up to the specified wait time if necessary for an element to become available.
     * @param timeout  how long to wait before giving up, in units of unit
     * @param unit     a TimeUnit determining how to interpret the timeout parameter
     * @return
     *
     * @throws InterruptedException
     */
    public boolean hasNext(long timeout, TimeUnit unit) throws InterruptedException {
        if (nextNotification == null) {
            nextNotification = gcNotifications.poll(timeout, unit);
        }
        return (nextNotification != null);
    }


    @Override
    public Notification next() {
        if (nextNotification == null) {
            nextNotification = gcNotifications.poll();
        }
        if (nextNotification == null) {
            throw new NoSuchElementException();
        }

        Notification retVal = nextNotification;
        nextNotification = null;
        return retVal;
    }

    @Override
    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

}