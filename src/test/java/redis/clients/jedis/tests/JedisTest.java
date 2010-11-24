package redis.clients.jedis.tests;

import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.tests.commands.JedisCommandTestBase;

public class JedisTest extends JedisCommandTestBase {
    @Test
    public void useWithoutConnecting() {
        Jedis jedis = new Jedis("localhost");
        jedis.auth("foobared");
        jedis.dbSize();
    }

    @Test
    public void connectWithShardInfo() {
        JedisShardInfo shardInfo = new JedisShardInfo("localhost",
                Protocol.DEFAULT_PORT);
        shardInfo.setPassword("foobared");
        Jedis jedis = new Jedis(shardInfo);
        jedis.get("foo");
    }
}
