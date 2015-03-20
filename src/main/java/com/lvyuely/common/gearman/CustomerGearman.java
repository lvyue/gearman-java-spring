package com.lvyuely.common.gearman;

import org.gearman.common.GearmanJobServerConnection;
import org.gearman.common.GearmanNIOJobServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class CustomerGearman extends Thread {

    private final static Logger logger = LoggerFactory.getLogger(CustomerGearman.class);

    /**
     * Spring Root Context
     */
    @Autowired
    private ApplicationContext applicationContext;

    /**
     * Gearman server host
     */
    private String host;

    /**
     * Gearman server port
     */
    private int port;

    /**
     * Gearman Functions
     */
    private Set<AbstractCustomGearmanFunction> functions = new HashSet<AbstractCustomGearmanFunction>();

    /**
     * Gearman 服务器连接
     */
    private GearmanJobServerConnection connection;

    /**
     * 自定义GearmanWorker实现
     */
    private CustomerGearmanWorkerImpl worker;

    /**
     * 初始化方法
     */
    @Override
    public void run() {
        // 创建gearman function
        connection = new GearmanNIOJobServerConnection(host, port);
        // 创建Worker
        worker = new CustomerGearmanWorkerImpl();
        // 设置链接
        worker.addServer(connection);
        //
        if (!functions.isEmpty()) {
            // 批量注册function
            for (AbstractCustomGearmanFunction f : functions) {
                // 注册function
                worker.registerFunction(f, f.getTimeout());
            }
        }
        worker.work();
    }

    /**
     * 销毁方法
     */
    public void finish() {
        // Gearman worker shutdown
        if (worker != null) {
            worker.stop();
            worker.shutdown();
        }
        // close gearman server connection
        if (connection != null) {
            connection.close();
        }

    }

    /**
     * Gearman 服务器地址
     *
     * @param host 服务器地址
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * gearman 服务器端口
     *
     * @param port 服务器端口
     */
    public void setPort(int port) {
        this.port = port;
    }


    /**
     * 设置function
     *
     * @param functions gearman函数类地址
     */
    public void setFunctions(List<Class<? extends AbstractCustomGearmanFunction>> functions) {
        // check function class exits and not null
        if (functions != null && !functions.isEmpty()) {
            // Iterator functions to get instance from spring root context
            for (Class<? extends AbstractCustomGearmanFunction> clazz : functions) {
                // get instance from Spring root context
                AbstractCustomGearmanFunction function = applicationContext.getBean(clazz);
                //function instance null check
                if (function != null) {
                    // add Function instance to Functions list
                    this.functions.add(function);
                }
            }
        }
    }
}
