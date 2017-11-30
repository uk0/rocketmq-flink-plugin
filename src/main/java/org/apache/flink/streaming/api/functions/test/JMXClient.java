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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class JMXClient {

    private static final String OBJECT_NAME_QUERY_PREFIX = "Q>";
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static void main(String[] args) throws Exception {

        String type = args[0];
        String host = args[1];
        String port = args[2];
        String user = args[3];
        String pass = args[4];
        String configFilename = args[5];
        int sleep   = Integer.parseInt(args[6]) * 1000;
        int expandMBeansNamesEvery = Integer.parseInt(args[7]);
        boolean performGCLogging = Boolean.parseBoolean(args[8]);
        int performThreadLoggingEvery = Integer.parseInt(args[9]);

        int expandMBeanNamesCounter = 0;
        int threadLoggingCounter = 0;

        HashMap<String, String[]> origConfig = readConfig(configFilename);
        if (origConfig == null) {
            System.exit(1);
        }


        HashMap<String, Object> env = new HashMap<String, Object>();
        String[] creds = new String[2];
        creds[0] = user;
        creds[1] = pass;
        env.put(JMXConnector.CREDENTIALS, creds);

        String urlString;
        if (type.equalsIgnoreCase("jboss")) {
            urlString = System.getProperty("jmx.service.url","service:jmx:remoting-jmx://" + host + ":" + port);
        }
        else {
            urlString = System.getProperty("jmx.service.url","service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi");
        }

        System.err.println("URL : " + urlString);
        JMXServiceURL serviceURL = new JMXServiceURL(urlString);
        JMXConnector jmxConnector = JMXConnectorFactory.connect(serviceURL, env);
        try {
            MBeanServerConnection connection = jmxConnector.getMBeanServerConnection();

            JMXGarbageCollectorNotification gcLogging = null;
            if (performGCLogging) {
                gcLogging = new JMXGarbageCollectorNotification(connection);
            }
            JMXThreadInfo threadLogging = null;
            if (performThreadLoggingEvery > 0) {
                threadLogging = new JMXThreadInfo();
            }

            HashMap<String, String[]> expandedConfig = null;

            do {

                // because this is a potentially expensive operation we will only perform it every @expandMBeanNamesEvery iteration
                if (expandMBeanNamesCounter <= 0 || expandedConfig == null) {
                    expandMBeanNamesCounter = expandMBeansNamesEvery;
                    expandedConfig = expandMBeanNames(connection, origConfig);
                }
                else {
                    expandMBeanNamesCounter--;
                }


                if (gcLogging != null) {
                    while (gcLogging.hasNext()) {
                        Notification notification = gcLogging.next();
                        System.out.printf("%s\t%s.%s\t%s\n", DATE_FORMAT.format(notification.getTimeStamp()), notification.getSource(), "occured", 1);
                    }
                }

                if (threadLogging != null) {
                    // because this is a potentially expensive operation we will only perform it every @performThreadLoggingEvery iteration
                    if (threadLoggingCounter <= 0) {
                        threadLoggingCounter = performThreadLoggingEvery;
                        String datetime = DATE_FORMAT.format(Calendar.getInstance().getTime());
                        Map<Thread.State, Long> threadStates = threadLogging.getCurrentInfo(connection);
                        long c = 0;
                        for(Map.Entry<Thread.State, Long> entry : threadStates.entrySet()) {
                            System.out.printf("%s\t%s.ThreadState.%s\t%s\n", datetime, JMXThreadInfo.MXBEAN_NAME, entry.getKey().name(), entry.getValue());
                            c += entry.getValue();
                        }
                        System.out.printf("%s\t%s.ThreadCount\t%s\n", datetime, JMXThreadInfo.MXBEAN_NAME, c);
                    }
                    else {
                        threadLoggingCounter--;
                    }
                }

                Iterator<Map.Entry<String, String[]>> it = expandedConfig.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String, String[]> pair = it.next();

                    ObjectName mbeanName = new ObjectName(pair.getKey());
                    String[] attrs = pair.getValue();

                    for(String attrName : attrs) {
                        Object attr = null;
                        try {
                            attr = connection.getAttribute(mbeanName, attrName);
                        }
                        catch (InstanceNotFoundException e) {
                        }
                        catch (MBeanException e) {
                        }
                        catch (AttributeNotFoundException e) {
                        }
                        String attrValue = (attr == null) ? "!null!" : attr.toString();
                        System.out.printf("%s\t%s.%s\t%s\n", DATE_FORMAT.format(Calendar.getInstance().getTime()), mbeanName, attrName, attrValue);
                    }

                }

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

    private static HashMap<String, String[]> readConfig(String configFilename) {

        HashMap<String, String[]> config = new LinkedHashMap<String, String[]>();

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(configFilename));

            String line;
            while ((line = br.readLine()) != null) {
                String[] pieces = line.split("\t", 2);
                String mbeanName = pieces[0];
                String[] attrs = pieces[1].split("\t");
                config.put(mbeanName, attrs);
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
     * The Configuration may contain MBean ObjectNames that are querys that need to be expanded before we can get the attributes for them.
     * If a configName starts with the {@value #OBJECT_NAME_QUERY_PREFIX} then the rest of the string is interpreted as a query to be run.
     * @param connection
     * @param config
     * @throws MalformedObjectNameException
     * @throws IOException
     * @return
     */
    private static HashMap<String, String[]> expandMBeanNames(MBeanServerConnection connection, HashMap<String, String[]> config) throws MalformedObjectNameException, IOException {

        HashMap<String, String[]> expandedConfig = new LinkedHashMap<String, String[]>(config.size());

        // Expand any Query config items at the beginning
        String[] configNames = config.keySet().toArray(new String[0]);
        for (String configName : configNames) {

            if (configName.startsWith(OBJECT_NAME_QUERY_PREFIX)) {
                Set<ObjectName> realNames = connection.queryNames(new ObjectName(configName.substring(OBJECT_NAME_QUERY_PREFIX.length())), null);
                if (realNames.size() > 0) {
                    for(ObjectName realName : realNames) {
                        System.err.printf("Query Found MBean %s\n", realName.getCanonicalName());
                        expandedConfig.put(realName.getCanonicalName(), config.get(configName));
                    }
                }
                else {
                    System.err.printf("Query Found zero (0) MBeans : %s\n", configName);
                }
            }
            else {
                expandedConfig.put(configName, config.get(configName));
            }

        }

        return expandedConfig;

    }

}