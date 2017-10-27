package com.scistor.Utils;

import com.scistor.Bean.DataInfo;

import java.util.List;

/**
 * Created by WANG Shenghua on 2017/10/27.
 */
public class TestSort {

    public static void main(String[] args) {
        List<DataInfo> dataInfos = RedisUtil.SortedHostByCount(RedisUtil.getRedisKeys());
        for (DataInfo dataInfo : dataInfos) {
            System.out.println(dataInfo);
        }
    }

}
