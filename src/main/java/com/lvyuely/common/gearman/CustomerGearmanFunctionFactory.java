package com.lvyuely.common.gearman;

import org.gearman.worker.AbstractGearmanFunction;
import org.gearman.worker.GearmanFunction;
import org.gearman.worker.GearmanFunctionFactory;

/**
 * Created by lvyue on 15/3/9.
 */
public class CustomerGearmanFunctionFactory implements GearmanFunctionFactory {

    private final String className;
    private final String functionName;
    private final AbstractGearmanFunction function;

    public CustomerGearmanFunctionFactory(AbstractGearmanFunction f) {
        // 判断是否唯恐
        if (f == null) {
            throw new IllegalStateException("");
        }
        // 设置对象
        this.function = f;
        // 设置class名称
        this.className = f.getClass().getName();
        // 获取对象名称
        String fname = f.getName();
        // 判断对象名称
        if (fname== null) {
            fname = className;
        } else {
            fname = fname.trim();
            if ("".equals(fname)) {
                fname = className;
            }
        }
        this.functionName = fname;
    }


    @Override
    public String getFunctionName() {
        return functionName;
    }

    @Override
    public GearmanFunction getFunction() {
        return function;
    }
}
