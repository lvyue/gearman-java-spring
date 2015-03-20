package com.lvyuely.common.gearman;

import org.apache.commons.lang3.StringUtils;
import org.gearman.util.ByteUtils;
import org.gearman.worker.AbstractGearmanFunction;

/**
 * Created by lvyue on 15/3/9.
 */
public abstract class AbstractCustomGearmanFunction extends AbstractGearmanFunction {

    /**
     * UTF－8字符编码格式
     */
    public static final String UTF_8_ENC = "UTF-8";

    /**
     * GBK
     */
    public static final String GBK_ENC = "GBK";


    /**
     * GB2312
     */
    public static final String GB2312_ENC = "GB2312";

    /**
     * 返回当前worker名称
     *
     * @return
     */
    public abstract String getName();


    /**
     * 返回gearman服务端传过来的数据，并以utf－8编码转换成String
     *
     * @return
     */
    public String getStringData() {
        return ByteUtils.fromUTF8Bytes((byte[]) this.data);
    }

    /**
     * 根据传入字符编码类型不同，将服务端数据转换为对应字符编码的字符串
     *
     * @param enc 字符编码类型，默认为UTF－8
     * @return
     */
    public String getStringData(String enc) {
        if (StringUtils.isBlank(enc)) {
            enc = UTF_8_ENC;
        }
        return ByteUtils.fromBytes((byte[]) this.data, enc);
    }

    /**
     * 获得服务端传过来的byte数据
     *
     * @return
     */
    public byte[] getByteData() {
        return (byte[]) this.data;
    }

    /**
     * 判断数据是否是空的
     *
     * @return
     */
    public boolean isEmptyData() {
        //
        return (this.data == null || ((byte[]) this.data).length == 0 || StringUtils.isBlank(getStringData()));
    }


    /**
     * 返回超时时间
     *
     * @return
     */
    public long getTimeout() {
        return 0l;
    }
}
