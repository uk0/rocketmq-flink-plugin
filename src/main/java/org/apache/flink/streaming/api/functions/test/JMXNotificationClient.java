package org.apache.flink.streaming.api.functions.test;

/**
 * Created by zhangjianxin on 2017/11/30.
 * Github Breakeval13
 * blog firsh.me
 */
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.management.AttributeChangeNotification;
import javax.management.MBeanServerConnection;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class JMXNotificationClient {

    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static void main(String[] args) throws Exception {

        String host = args[0];
        String port = args[1];
        String user = args[2];
        String pass = args[3];
        String configFilename = args[4];
        int sleep   = Integer.parseInt(args[5]) * 1000;

        // HashMap<String, String[]> config = readConfig(configFilename);
        List<String> config = readConfig(configFilename);


        HashMap<String, Object> env = new HashMap<String, Object>();
        String[] creds = new String[2];
        creds[0] = user;
        creds[1] = pass;
        env.put(JMXConnector.CREDENTIALS, creds);

        ManagementFactory.getGarbageCollectorMXBeans();

        String urlString = System.getProperty("jmx.service.url","service:jmx:remoting-jmx://" + host + ":" + port);
        System.err.println("URL : " + urlString);
        JMXServiceURL serviceURL = new JMXServiceURL(urlString);
        JMXConnector jmxConnector = JMXConnectorFactory.connect(serviceURL, env);
        try {
            MBeanServerConnection connection = jmxConnector.getMBeanServerConnection();

            ClientListener listener = new ClientListener();

            Iterator<String> it = config.iterator();
            while (it.hasNext()) {
                String name = it.next();
                ObjectName mbeanName = new ObjectName(name);
                System.err.printf("Add Listener for %s\n", name);
                connection.addNotificationListener(mbeanName, listener, null, name);
            }

            do {
                System.out.printf("<timestamp>%s</timestamp>\n", DATE_FORMAT.format(Calendar.getInstance().getTime()));
                Thread.sleep(sleep);
            } while (true);

        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            jmxConnector.close();
        }

    }



    private static List<String> readConfig(String configFilename) {

        List<String> config = new LinkedList<String>();

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(configFilename));

            String line;
            while ((line = br.readLine()) != null) {
                config.add(line);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
            config = null;
        } catch (IOException e) {
            e.printStackTrace();
            config = null;
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return config;

    }

    /**
     * Inner class that will handle the notifications.
     */
    public static class ClientListener implements NotificationListener {
        @Override
        public void handleNotification(Notification notification, Object handback) {
            echo("\n<notification>");
            echo("\t<currentDatetime>" + DATE_FORMAT.format(Calendar.getInstance().getTime()) + "</currentDatetime>");
            echo("\t<notificationTimestamp>" + notification.getTimeStamp() + "</notificationTimestamp>");
            echo("\t<notificationDatetime>" + DATE_FORMAT.format(new Date(notification.getTimeStamp())) + "</notificationDatetime>");
            echo("\t<configName>" + handback.toString() + "</configName>");
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
            echo("\n</notification>");
        }

    }

    private static void echo(String msg) {
        System.out.println(msg);
    }



}