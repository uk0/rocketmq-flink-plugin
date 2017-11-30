package org.apache.flink.streaming.api.functions.jmx;

/**
 * Created by zhangjianxin on 2017/11/30.
 * Github Breakeval13
 * blog firsh.me
 */
public class Hello implements HelloMBean {
    private String name;
    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void printHello() {
        System.out.println("Hello world, "+ name);
    }

    @Override
    public void printHello(String whoName) {
        System.out.println("Hello, "+whoName);
    }
}
