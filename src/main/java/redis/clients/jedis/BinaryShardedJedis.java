package redis.clients.jedis;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import redis.clients.util.Hashing;
import redis.clients.util.Sharded;

public class BinaryShardedJedis extends Sharded<Jedis, JedisShardInfo>
        implements BinaryJedisCommands {
    public BinaryShardedJedis(List<JedisShardInfo> shards) {
        super(shards);
    }

    public BinaryShardedJedis(List<JedisShardInfo> shards, Hashing algo) {
        super(shards, algo);
    }

    public BinaryShardedJedis(List<JedisShardInfo> shards, Pattern keyTagPattern) {
        super(shards, keyTagPattern);
    }

    public BinaryShardedJedis(List<JedisShardInfo> shards, Hashing algo,
            Pattern keyTagPattern) {
        super(shards, algo, keyTagPattern);
    }

    public void disconnect() throws IOException {
        for (Jedis jedis : getAllShards()) {
            jedis.disconnect();
        }
    }

    protected Jedis create(JedisShardInfo shard) {
        return new Jedis(shard);
    }

    public String set(byte[] key, byte[] value) {
        Jedis j = getShard(key);
        return j.set(key, value);
    }

    public byte[] get(byte[] key) {
        Jedis j = getShard(key);
        return j.get(key);
    }

    public Long exists(byte[] key) {
        Jedis j = getShard(key);
        return j.exists(key);
    }

    public String type(byte[] key) {
        Jedis j = getShard(key);
        return j.type(key);
    }

    public Long expire(byte[] key, int seconds) {
        Jedis j = getShard(key);
        return j.expire(key, seconds);
    }

    public Long expireAt(byte[] key, long unixTime) {
        Jedis j = getShard(key);
        return j.expireAt(key, unixTime);
    }

    public Long ttl(byte[] key) {
        Jedis j = getShard(key);
        return j.ttl(key);
    }

    public byte[] getSet(byte[] key, byte[] value) {
        Jedis j = getShard(key);
        return j.getSet(key, value);
    }

    public Long setnx(byte[] key, byte[] value) {
        Jedis j = getShard(key);
        return j.setnx(key, value);
    }

    public Long decrBy(byte[] key, int integer) {
        Jedis j = getShard(key);
        return j.decrBy(key, integer);
    }

    public Long decr(byte[] key) {
        Jedis j = getShard(key);
        return j.decr(key);
    }

    public Long incrBy(byte[] key, int integer) {
        Jedis j = getShard(key);
        return j.incrBy(key, integer);
    }

    public Long incr(byte[] key) {
        Jedis j = getShard(key);
        return j.incr(key);
    }

    public String rpush(byte[] key, byte[] string) {
        Jedis j = getShard(key);
        return j.rpush(key, string);
    }

    public String lpush(byte[] key, byte[] string) {
        Jedis j = getShard(key);
        return j.lpush(key, string);
    }

    public Long llen(byte[] key) {
        Jedis j = getShard(key);
        return j.llen(key);
    }

    public List<byte[]> lrange(byte[] key, int start, int end) {
        Jedis j = getShard(key);
        return j.lrange(key, start, end);
    }

    public String ltrim(byte[] key, int start, int end) {
        Jedis j = getShard(key);
        return j.ltrim(key, start, end);
    }

    public byte[] lindex(byte[] key, int index) {
        Jedis j = getShard(key);
        return j.lindex(key, index);
    }

    public String lset(byte[] key, int index, byte[] value) {
        Jedis j = getShard(key);
        return j.lset(key, index, value);
    }

    public Long lrem(byte[] key, int count, byte[] value) {
        Jedis j = getShard(key);
        return j.lrem(key, count, value);
    }

    public byte[] lpop(byte[] key) {
        Jedis j = getShard(key);
        return j.lpop(key);
    }

    public byte[] rpop(byte[] key) {
        Jedis j = getShard(key);
        return j.rpop(key);
    }

    public Long sadd(byte[] key, byte[] member) {
        Jedis j = getShard(key);
        return j.sadd(key, member);
    }

    public Set<byte[]> smembers(byte[] key) {
        Jedis j = getShard(key);
        return j.smembers(key);
    }

    public Long srem(byte[] key, byte[] member) {
        Jedis j = getShard(key);
        return j.srem(key, member);
    }

    public byte[] spop(byte[] key) {
        Jedis j = getShard(key);
        return j.spop(key);
    }

    public Long scard(byte[] key) {
        Jedis j = getShard(key);
        return j.scard(key);
    }

    public Long sismember(byte[] key, byte[] member) {
        Jedis j = getShard(key);
        return j.sismember(key, member);
    }

    public byte[] srandmember(byte[] key) {
        Jedis j = getShard(key);
        return j.srandmember(key);
    }

    public Long zadd(byte[] key, double score, byte[] member) {
        Jedis j = getShard(key);
        return j.zadd(key, score, member);
    }

    public Set<byte[]> zrange(byte[] key, int start, int end) {
        Jedis j = getShard(key);
        return j.zrange(key, start, end);
    }

    public Long zrem(byte[] key, byte[] member) {
        Jedis j = getShard(key);
        return j.zrem(key, member);
    }

    public Double zincrby(byte[] key, double score, byte[] member) {
        Jedis j = getShard(key);
        return j.zincrby(key, score, member);
    }

    public Set<byte[]> zrevrange(byte[] key, int start, int end) {
        Jedis j = getShard(key);
        return j.zrevrange(key, start, end);
    }

    public Set<Tuple> zrangeWithScores(byte[] key, int start, int end) {
        Jedis j = getShard(key);
        return j.zrangeWithScores(key, start, end);
    }

    public Set<Tuple> zrevrangeWithScores(byte[] key, int start, int end) {
        Jedis j = getShard(key);
        return j.zrevrangeWithScores(key, start, end);
    }

    public Long zcard(byte[] key) {
        Jedis j = getShard(key);
        return j.zcard(key);
    }

    public Double zscore(byte[] key, byte[] member) {
        Jedis j = getShard(key);
        return j.zscore(key, member);
    }

    public List<byte[]> sort(byte[] key) {
        Jedis j = getShard(key);
        return j.sort(key);
    }

    public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
        Jedis j = getShard(key);
        return j.sort(key, sortingParameters);
    }

    public Long zcount(byte[] key, double min, double max) {
        Jedis j = getShard(key);
        return j.zcount(key, min, max);
    }

    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
        Jedis j = getShard(key);
        return j.zrangeByScore(key, min, max);
    }

    public Set<byte[]> zrangeByScore(byte[] key, double min, double max,
            int offset, int count) {
        Jedis j = getShard(key);
        return j.zrangeByScore(key, min, max, offset, count);
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
        Jedis j = getShard(key);
        return j.zrangeByScoreWithScores(key, min, max);
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min,
            double max, int offset, int count) {
        Jedis j = getShard(key);
        return j.zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Long zremrangeByScore(byte[] key, double start, double end) {
        Jedis j = getShard(key);
        return j.zremrangeByScore(key, start, end);
    }

    public List<Object> pipelined(ShardedJedisPipeline shardedJedisPipeline) {
        shardedJedisPipeline.setShardedJedis(this);
        shardedJedisPipeline.execute();
        return shardedJedisPipeline.getResults();
    }
}