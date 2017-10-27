package Util;

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.util.Map;
import java.util.TreeSet;

public class RedisOperator implements IRedisOperater {

    public final static Logger logger = Logger.getLogger(RedisOperator.class);

    private JedisCluster jedisCluster;

    public RedisOperator(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }

    @Override
    public TreeSet<String> keys(String pattern){
        logger.debug("Start getting keys...");
        TreeSet<String> keys = new TreeSet<String>();
        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();
        for(String k : clusterNodes.keySet()){
            logger.debug("Getting keys from: " + k);
            JedisPool jp = clusterNodes.get(k);
            Jedis connection = jp.getResource();
            try {
                keys.addAll(connection.keys(pattern));
            } catch(Exception e){
                logger.error("Getting keys error: {}", e);
            } finally{
                logger.debug("Connection closed.");
                connection.close();//用完一定要close这个链接！！！
            }
        }
        logger.debug("Keys gotten!");
        return keys;
    }
}