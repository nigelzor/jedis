package redis.clients.jedis;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import redis.clients.util.Hashing;

public class ShardedJedis extends BinaryShardedJedis implements JedisCommands {
    public ShardedJedis(List<JedisShardInfo> shards) {
        super(shards);
    }

    public ShardedJedis(List<JedisShardInfo> shards, Hashing algo) {
        super(shards, algo);
    }

    public ShardedJedis(List<JedisShardInfo> shards, Pattern keyTagPattern) {
        super(shards, keyTagPattern);
    }

    public ShardedJedis(List<JedisShardInfo> shards, Hashing algo,
            Pattern keyTagPattern) {
        super(shards, algo, keyTagPattern);
    }

    @Override
	public void disconnect() throws IOException {
        for (Jedis jedis : getAllShards()) {
            jedis.quit();
            jedis.disconnect();
        }
    }

    public String set(String key, String value) {
        Jedis j = getShard(key);
        return j.set(key, value);
    }

    public String get(String key) {
        Jedis j = getShard(key);
        return j.get(key);
    }

    public Long exists(String key) {
        Jedis j = getShard(key);
        return j.exists(key);
    }

    public String type(String key) {
        Jedis j = getShard(key);
        return j.type(key);
    }

    public Long expire(String key, int seconds) {
        Jedis j = getShard(key);
        return j.expire(key, seconds);
    }

    public Long expireAt(String key, long unixTime) {
        Jedis j = getShard(key);
        return j.expireAt(key, unixTime);
    }

    public Long ttl(String key) {
        Jedis j = getShard(key);
        return j.ttl(key);
    }

    public String getSet(String key, String value) {
        Jedis j = getShard(key);
        return j.getSet(key, value);
    }

    public Long setnx(String key, String value) {
        Jedis j = getShard(key);
        return j.setnx(key, value);
    }

    public Long decrBy(String key, int integer) {
        Jedis j = getShard(key);
        return j.decrBy(key, integer);
    }

    public Long decr(String key) {
        Jedis j = getShard(key);
        return j.decr(key);
    }

    public Long incrBy(String key, int integer) {
        Jedis j = getShard(key);
        return j.incrBy(key, integer);
    }

    public Long incr(String key) {
        Jedis j = getShard(key);
        return j.incr(key);
    }

    public String rpush(String key, String string) {
        Jedis j = getShard(key);
        return j.rpush(key, string);
    }

    public String lpush(String key, String string) {
        Jedis j = getShard(key);
        return j.lpush(key, string);
    }

    public Long llen(String key) {
        Jedis j = getShard(key);
        return j.llen(key);
    }

    public List<String> lrange(String key, int start, int end) {
        Jedis j = getShard(key);
        return j.lrange(key, start, end);
    }

    public String ltrim(String key, int start, int end) {
        Jedis j = getShard(key);
        return j.ltrim(key, start, end);
    }

    public String lindex(String key, int index) {
        Jedis j = getShard(key);
        return j.lindex(key, index);
    }

    public String lset(String key, int index, String value) {
        Jedis j = getShard(key);
        return j.lset(key, index, value);
    }

    public Long lrem(String key, int count, String value) {
        Jedis j = getShard(key);
        return j.lrem(key, count, value);
    }

    public String lpop(String key) {
        Jedis j = getShard(key);
        return j.lpop(key);
    }

    public String rpop(String key) {
        Jedis j = getShard(key);
        return j.rpop(key);
    }

    public Long sadd(String key, String member) {
        Jedis j = getShard(key);
        return j.sadd(key, member);
    }

    public Set<String> smembers(String key) {
        Jedis j = getShard(key);
        return j.smembers(key);
    }

    public Long srem(String key, String member) {
        Jedis j = getShard(key);
        return j.srem(key, member);
    }

    public String spop(String key) {
        Jedis j = getShard(key);
        return j.spop(key);
    }

    public Long scard(String key) {
        Jedis j = getShard(key);
        return j.scard(key);
    }

    public Long sismember(String key, String member) {
        Jedis j = getShard(key);
        return j.sismember(key, member);
    }

    public String srandmember(String key) {
        Jedis j = getShard(key);
        return j.srandmember(key);
    }

    public Long zadd(String key, double score, String member) {
        Jedis j = getShard(key);
        return j.zadd(key, score, member);
    }

    public Set<String> zrange(String key, int start, int end) {
        Jedis j = getShard(key);
        return j.zrange(key, start, end);
    }

    public Long zrem(String key, String member) {
        Jedis j = getShard(key);
        return j.zrem(key, member);
    }

    public Double zincrby(String key, double score, String member) {
        Jedis j = getShard(key);
        return j.zincrby(key, score, member);
    }

    public Set<String> zrevrange(String key, int start, int end) {
        Jedis j = getShard(key);
        return j.zrevrange(key, start, end);
    }

    public Set<Tuple> zrangeWithScores(String key, int start, int end) {
        Jedis j = getShard(key);
        return j.zrangeWithScores(key, start, end);
    }

    public Set<Tuple> zrevrangeWithScores(String key, int start, int end) {
        Jedis j = getShard(key);
        return j.zrevrangeWithScores(key, start, end);
    }

    public Long zcard(String key) {
        Jedis j = getShard(key);
        return j.zcard(key);
    }

    public Double zscore(String key, String member) {
        Jedis j = getShard(key);
        return j.zscore(key, member);
    }

    public List<String> sort(String key) {
        Jedis j = getShard(key);
        return j.sort(key);
    }

    public List<String> sort(String key, SortingParams sortingParameters) {
        Jedis j = getShard(key);
        return j.sort(key, sortingParameters);
    }

    public Long zcount(String key, double min, double max) {
        Jedis j = getShard(key);
        return j.zcount(key, min, max);
    }

    public Set<String> zrangeByScore(String key, double min, double max) {
        Jedis j = getShard(key);
        return j.zrangeByScore(key, min, max);
    }

    public Set<String> zrangeByScore(String key, double min, double max,
            int offset, int count) {
        Jedis j = getShard(key);
        return j.zrangeByScore(key, min, max, offset, count);
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        Jedis j = getShard(key);
        return j.zrangeByScoreWithScores(key, min, max);
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, double min,
            double max, int offset, int count) {
        Jedis j = getShard(key);
        return j.zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Long zremrangeByScore(String key, double start, double end) {
        Jedis j = getShard(key);
        return j.zremrangeByScore(key, start, end);
    }
}