package com.scistor.Utils;

import com.scistor.Bean.DataInfo;
import com.scistor.Main;
import javafx.scene.chart.PieChart;
import org.apache.log4j.Logger;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.util.*;

/**
 * Created by WANG Shenghua on 2017/10/27.
 */
public class RedisUtil {

    private static JedisCluster jedisCluster = null;
    private static final Logger logger = Logger.getLogger(RedisUtil.class);
    private static final String KEY1 = "COUNT";
    private static final String KEY2 = "STATUS";

    static {
        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
        jedisClusterNodes.add(new HostAndPort("172.16.18.234", 7000));
        jedisClusterNodes.add(new HostAndPort("172.16.18.234", 7001));
        jedisClusterNodes.add(new HostAndPort("172.16.18.234", 7002));
        jedisClusterNodes.add(new HostAndPort("172.16.18.234", 7003));
        jedisClusterNodes.add(new HostAndPort("172.16.18.234", 7004));
        jedisClusterNodes.add(new HostAndPort("172.16.18.234", 7005));
        jedisCluster = new JedisCluster(jedisClusterNodes);
    }

    public static TreeSet<String> getRedisKeys () {
        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();
        TreeSet<String> keys = new TreeSet<String>();
        for(String k : clusterNodes.keySet()){
            logger.debug("Getting keys from: " + k);
            JedisPool jp = clusterNodes.get(k);
            Jedis connection = jp.getResource();
            try {
                keys.addAll(connection.keys("*"));
            } catch(Exception e){
                logger.error("Getting keys error: {}", e);
            } finally{
                logger.debug("Connection closed.");
                connection.close();
            }
        }
        return keys;
    }

    public static List<DataInfo> SortedHostByCount(TreeSet<String> hosts) {
        int totalCount = 0;
        List<DataInfo> dataInfos = new ArrayList<DataInfo>();
        Iterator<String> it = hosts.iterator();
        while (it.hasNext()) {
            String host = it.next();
            List<String> hmget = jedisCluster.hmget(host, KEY1, KEY2);
            if (null != hmget && hmget.size() > 0) {
                dataInfos.add(new DataInfo(host, Integer.parseInt(hmget.get(0)), hmget.get(1)));
                totalCount = totalCount + Integer.parseInt(hmget.get(0));
            }
        }
        Collections.sort(dataInfos, new Comparator<DataInfo>() {
            public int compare(DataInfo o1, DataInfo o2) {
                int count1 = o1.getCOUNT();
                int count2 = o2.getCOUNT();
                if (count1 > count2) {
                    return -1;
                } else if (count1 < count2) {
                    return  1;
                } else  {
                    return o1.getHOST().compareTo(o2.getHOST());
                }
            }
        });
        System.out.println(totalCount);
        return  dataInfos;
    }

}
