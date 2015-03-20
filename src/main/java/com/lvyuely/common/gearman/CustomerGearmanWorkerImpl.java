package com.lvyuely.common.gearman;

import org.gearman.worker.GearmanWorkerImpl;

/**
 * Created by lvyue on 15/3/9.
 */
public class CustomerGearmanWorkerImpl extends GearmanWorkerImpl {

    /**
     * 注册方法对象
     * @param function
     * @param timeout
     */
    public void registerFunction(AbstractCustomGearmanFunction function, long timeout) {
        registerFunctionFactory(new CustomerGearmanFunctionFactory(function),
                timeout);
    }

    public void registerFunction(AbstractCustomGearmanFunction function) {
        registerFunctionFactory(new CustomerGearmanFunctionFactory(function),
                0l);
    }

}
