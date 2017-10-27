import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RedisWhiteListInit {

    private JedisCluster getJedisCluster() {
        Set<HostAndPort> jedisClusterNodes = new HashSet<>();
        jedisClusterNodes.add(new HostAndPort("172.16.18.234", 7000));
        jedisClusterNodes.add(new HostAndPort("172.16.18.234", 7001));
        jedisClusterNodes.add(new HostAndPort("172.16.18.234", 7002));
        jedisClusterNodes.add(new HostAndPort("172.16.18.234", 7003));
        jedisClusterNodes.add(new HostAndPort("172.16.18.234", 7004));
        jedisClusterNodes.add(new HostAndPort("172.16.18.234", 7005));
        JedisCluster jCluster = new JedisCluster(jedisClusterNodes);
        return jCluster;
    }

    public static void main(String[] args) throws Exception {
        RedisWhiteListInit init = new RedisWhiteListInit();
        JedisCluster jCluster = init.getJedisCluster();
        if (args.length < 1) {
            System.out.println("请传入初始白名单所在路径！");
            return;
        }
        String FilePath = args[0];
        File file = new File(FilePath);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        String host;
        while ((host = bufferedReader.readLine()) != null) {
            Map<String, String> params = new HashMap<>();
            params.put("COUNT", "0");
            params.put("STATUS", "1");
            jCluster.hmset(host, params);
        }
        jCluster.close();
    }

}
