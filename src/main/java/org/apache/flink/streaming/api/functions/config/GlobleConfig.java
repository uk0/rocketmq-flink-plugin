package org.apache.flink.streaming.api.functions.config;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/**
 * Created by zhangjianxin on 2017/11/29.
 * Github Breakeval13
 * blog firsh.me
 */
public class GlobleConfig {
    private static GlobleConfig instance;
    public static JSONObject all;
    private static String mqaddr;
    public static String confdir;
    private static String hdfsConfig;


    private GlobleConfig() {
        File confFile = new File(confdir + File.separator + "conf.json");
        Scanner confFileScanner = null;
        StringBuilder buffer = new StringBuilder();
        try {
            confFileScanner = new Scanner(confFile, "utf-8");
            while (confFileScanner.hasNextLine()) {
                buffer.append(confFileScanner.nextLine());
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();

        } finally {
            if (confFileScanner != null) {
                confFileScanner.close();
            }
        }
        all = JSON.parseObject(buffer.toString());
    }

    public static synchronized GlobleConfig getInstance(String path) {

        if (instance == null) {
            confdir = path;
            instance = new GlobleConfig();
        }
        return instance;
    }

    public static String getMQ() {
        if (all != null) {
            mqaddr = all.getString("mq");
        }
        return mqaddr;
    }

    public static JSONObject getHDFSConfig() {
        if (all != null) {
            hdfsConfig = all.getString("hdfs");
        }
        return JSON.parseObject(hdfsConfig);
    }

    public static void main(String[] args) {
        GlobleConfig.getInstance("your config dir path ");
        System.out.println(getMQ());
    }
}
