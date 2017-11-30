package org.apache.flink.streaming.api.functions.test;

/**
 * Created by zhangjianxin on 2017/11/30.
 * Github Breakeval13
 * blog firsh.me
 */
import java.lang.management.MemoryUsage;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import javax.management.AttributeChangeNotification;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.Notification;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.sun.management.GarbageCollectorMXBean;
import com.sun.management.GcInfo;

public class JMXGarbageCollectorNotificationClient {

    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static void main(String[] args) throws Exception {

        String host = args[0];
        String port = args[1];
        String user = args[2];
        String pass = args[3];
        int pollSleep = 300;

        HashMap<String, Object> env = new HashMap<String, Object>();
        String[] creds = new String[2];
        creds[0] = user;
        creds[1] = pass;
        env.put(JMXConnector.CREDENTIALS, creds);

        String urlString = System.getProperty("jmx.service.url","service:jmx:remoting-jmx://" + host + ":" + port);
        System.err.println("URL : " + urlString);
        JMXServiceURL serviceURL = new JMXServiceURL(urlString);
        JMXConnector jmxConnector = JMXConnectorFactory.connect(serviceURL, env);
        try {
            MBeanServerConnection connection = jmxConnector.getMBeanServerConnection();

            JMXGarbageCollectorNotification gcLogging = new JMXGarbageCollectorNotification(connection);

            do {
                while (gcLogging.hasNext(pollSleep, TimeUnit.SECONDS)) {
                    Notification notification = gcLogging.next();
                    processNotification(connection, notification);
                }
                System.out.printf("<timestamp>%s</timestamp>\n", DATE_FORMAT.format(Calendar.getInstance().getTime()));

                // make a call using the connection to check it is still active, otherwise we never notice.
                connection.getDefaultDomain();
            } while (true);

        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            jmxConnector.close();
        }

    }


    public static void processNotification(MBeanServerConnection connection, Notification notification) {
        echo("\n<notification>");
        echo("\t<currentDatetime>" + DATE_FORMAT.format(Calendar.getInstance().getTime()) + "</currentDatetime>");
        echo("\t<notificationTime timestamp='" + notification.getTimeStamp() + "'>" + DATE_FORMAT.format(new Date(notification.getTimeStamp())) + "</notificationTime>");
        echo("\t<className>" + notification.getClass().getName() + "</className>");
        echo("\t<source>" + notification.getSource() + "</source>");
        echo("\t<type>" + notification.getType() + "</type>");
        echo("\t<message>" + notification.getMessage() + "</message>");
        if (notification instanceof AttributeChangeNotification) {
            echo("\t<attributeChanges>");
            AttributeChangeNotification acn = (AttributeChangeNotification) notification;
            echo("\t\t<attributeName>" + acn.getAttributeName() + "</attributeName>");
            echo("\t\t<attributeType>" + acn.getAttributeType() + "</attributeType>");
            echo("\t\t<newValue>" + acn.getNewValue() + "</newValue>");
            echo("\t\t<oldValue>" + acn.getOldValue() + "</oldValue>");
            echo("\t</attributeChanges>");
        }

        if (notification.getSource() instanceof ObjectName) {
            GarbageCollectorMXBean gcBean = JMX.newMXBeanProxy(connection, (ObjectName) notification.getSource(), GarbageCollectorMXBean.class);
            echo("\t<gcInfo>");
            echo("\t\t<collectionTime>" + gcBean.getCollectionTime() + "</collectionTime>");
            echo("\t\t<collectionCount>" + gcBean.getCollectionCount() + "</collectionCount>");
            GcInfo gcInfo = gcBean.getLastGcInfo();
            echo("\t\t<startTime>" + gcInfo.getStartTime() + "</startTime>");
            echo("\t\t<endTime>"   + gcInfo.getEndTime()   + "</endTime>");
            echo("\t\t<duration>"  + gcInfo.getDuration()  + "</duration>");
            outputMemoryUsages(gcInfo.getMemoryUsageBeforeGc(), "memoryUsageBeforeGC");
            outputMemoryUsages(gcInfo.getMemoryUsageAfterGc(), "memoryUsageAfterGC");
            echo("\t</gcInfo>");
        }

        echo("\n</notification>");
    }

    private static void outputMemoryUsages(Map<String, MemoryUsage> memUsages, String xmlName) {
        echo("\t\t<" + xmlName + ">");
        for (Entry<String, MemoryUsage> memUsage : memUsages.entrySet()) {
            MemoryUsage mu = memUsage.getValue();
            echo("\t\t\t<pool>" +
                    "<name>" + memUsage.getKey() + "</name><init>" + mu.getInit() + "</init><used>" + mu.getUsed() +
                    "</used><max>" + mu.getMax() + "</max><committed>" + mu.getCommitted() + "</committed></pool>");
        }
        echo("\t\t</" + xmlName + ">");
    }

    private static void echo(String msg) {
        System.out.println(msg);
    }



}