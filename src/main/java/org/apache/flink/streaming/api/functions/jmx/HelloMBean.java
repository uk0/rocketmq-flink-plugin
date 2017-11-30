package org.apache.flink.streaming.api.functions.jmx;

/**
 * Created by zhangjianxin on 2017/11/30.
 * Github Breakeval13
 * blog firsh.me
 */

public interface HelloMBean {
    public String getName();
    public void setName(String name);
    public void printHello();
    public void printHello(String whoName);
}