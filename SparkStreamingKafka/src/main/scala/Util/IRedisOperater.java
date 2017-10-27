package Util;

import java.util.TreeSet;

public interface IRedisOperater {

    /**
     * 根据pattern 获取所有的keys
     * @param pattern
     * @return
     */
    TreeSet<String> keys(String pattern);
}