package redis.clients.jedis;

import java.util.List;
import java.util.Set;

/**
 * Common interface for sharded and non-sharded BinaryJedis
 */
public interface BinaryJedisCommands {
    String set(byte[] key, byte[] value);

    byte[] get(byte[] key);

    Long exists(byte[] key);

    String type(byte[] key);

    Long expire(byte[] key, int seconds);

    Long expireAt(byte[] key, long unixTime);

    Long ttl(byte[] key);

    byte[] getSet(byte[] key, byte[] value);

    Long setnx(byte[] key, byte[] value);

    Long decrBy(byte[] key, int integer);

    Long decr(byte[] key);

    Long incrBy(byte[] key, int integer);

    Long incr(byte[] key);

    String rpush(byte[] key, byte[] string);

    String lpush(byte[] key, byte[] string);

    Long llen(byte[] key);

    List<byte[]> lrange(byte[] key, int start, int end);

    String ltrim(byte[] key, int start, int end);

    byte[] lindex(byte[] key, int index);

    String lset(byte[] key, int index, byte[] value);

    Long lrem(byte[] key, int count, byte[] value);

    byte[] lpop(byte[] key);

    byte[] rpop(byte[] key);

    Long sadd(byte[] key, byte[] member);

    Set<byte[]> smembers(byte[] key);

    Long srem(byte[] key, byte[] member);

    byte[] spop(byte[] key);

    Long scard(byte[] key);

    Long sismember(byte[] key, byte[] member);

    byte[] srandmember(byte[] key);

    Long zadd(byte[] key, double score, byte[] member);

    Set<byte[]> zrange(byte[] key, int start, int end);

    Long zrem(byte[] key, byte[] member);

    Double zincrby(byte[] key, double score, byte[] member);

    Set<byte[]> zrevrange(byte[] key, int start, int end);

    Set<Tuple> zrangeWithScores(byte[] key, int start, int end);

    Set<Tuple> zrevrangeWithScores(byte[] key, int start, int end);

    Long zcard(byte[] key);

    Double zscore(byte[] key, byte[] member);

    List<byte[]> sort(byte[] key);

    List<byte[]> sort(byte[] key, SortingParams sortingParameters);

    Long zcount(byte[] key, double min, double max);

    Set<byte[]> zrangeByScore(byte[] key, double min, double max);

    Set<byte[]> zrangeByScore(
    		byte[] key,
    		double min,
    		double max,
    		int offset,
    		int count);

    Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max);

    Set<Tuple> zrangeByScoreWithScores(
    		byte[] key,
    		double min,
            double max,
            int offset,
            int count);

    Long zremrangeByScore(byte[] key, double start, double end);

}
