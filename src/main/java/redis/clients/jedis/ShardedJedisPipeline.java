package redis.clients.jedis;

import java.util.ArrayList;
import java.util.List;

public abstract class ShardedJedisPipeline {
    private BinaryShardedJedis jedis;
    private List<FutureResult> results = new ArrayList<FutureResult>();

    private class FutureResult {
        private Client client;

        public FutureResult(Client client) {
            this.client = client;
        }

        public Object get() {
            return client.getOne();
        }
    }

    public void setShardedJedis(BinaryShardedJedis jedis) {
        this.jedis = jedis;
    }

    protected void set(String key, String value) {
        Client c = jedis.getShard(key).getClient();
        c.set(key, value);
        results.add(new FutureResult(c));
    }

    protected void get(String key) {
        Client c = jedis.getShard(key).getClient();
        c.get(key);
        results.add(new FutureResult(c));
    }

    protected void exists(String key) {
        Client c = jedis.getShard(key).getClient();
        c.exists(key);
        results.add(new FutureResult(c));
    }

    protected void type(String key) {
        Client c = jedis.getShard(key).getClient();
        c.type(key);
        results.add(new FutureResult(c));
    }

    protected void expire(String key, int seconds) {
        Client c = jedis.getShard(key).getClient();
        c.expire(key, seconds);
        results.add(new FutureResult(c));
    }

    protected void expireAt(String key, long unixTime) {
        Client c = jedis.getShard(key).getClient();
        c.expireAt(key, unixTime);
        results.add(new FutureResult(c));
    }

    protected void ttl(String key) {
        Client c = jedis.getShard(key).getClient();
        c.ttl(key);
        results.add(new FutureResult(c));
    }

    protected void getSet(String key, String value) {
        Client c = jedis.getShard(key).getClient();
        c.getSet(key, value);
        results.add(new FutureResult(c));
    }

    protected void setnx(String key, String value) {
        Client c = jedis.getShard(key).getClient();
        c.setnx(key, value);
        results.add(new FutureResult(c));
    }

    protected void decrBy(String key, int integer) {
        Client c = jedis.getShard(key).getClient();
        c.decrBy(key, integer);
        results.add(new FutureResult(c));
    }

    protected void decr(String key) {
        Client c = jedis.getShard(key).getClient();
        c.decr(key);
        results.add(new FutureResult(c));
    }

    protected void incrBy(String key, int integer) {
        Client c = jedis.getShard(key).getClient();
        c.incrBy(key, integer);
        results.add(new FutureResult(c));
    }

    protected void incr(String key) {
        Client c = jedis.getShard(key).getClient();
        c.incr(key);
        results.add(new FutureResult(c));
    }

    protected void rpush(String key, String string) {
        Client c = jedis.getShard(key).getClient();
        c.rpush(key, string);
        results.add(new FutureResult(c));
    }

    protected void lpush(String key, String string) {
        Client c = jedis.getShard(key).getClient();
        c.lpush(key, string);
        results.add(new FutureResult(c));
    }

    protected void llen(String key) {
        Client c = jedis.getShard(key).getClient();
        c.llen(key);
        results.add(new FutureResult(c));
    }

    protected void lrange(String key, int start, int end) {
        Client c = jedis.getShard(key).getClient();
        c.lrange(key, start, end);
        results.add(new FutureResult(c));
    }

    protected void ltrim(String key, int start, int end) {
        Client c = jedis.getShard(key).getClient();
        c.ltrim(key, start, end);
        results.add(new FutureResult(c));
    }

    protected void lindex(String key, int index) {
        Client c = jedis.getShard(key).getClient();
        c.lindex(key, index);
        results.add(new FutureResult(c));
    }

    protected void lset(String key, int index, String value) {
        Client c = jedis.getShard(key).getClient();
        c.lset(key, index, value);
        results.add(new FutureResult(c));
    }

    protected void lrem(String key, int count, String value) {
        Client c = jedis.getShard(key).getClient();
        c.lrem(key, count, value);
        results.add(new FutureResult(c));
    }

    protected void lpop(String key) {
        Client c = jedis.getShard(key).getClient();
        c.lpop(key);
        results.add(new FutureResult(c));
    }

    protected void rpop(String key) {
        Client c = jedis.getShard(key).getClient();
        c.rpop(key);
        results.add(new FutureResult(c));
    }

    protected void sadd(String key, String member) {
        Client c = jedis.getShard(key).getClient();
        c.sadd(key, member);
        results.add(new FutureResult(c));
    }

    protected void smembers(String key) {
        Client c = jedis.getShard(key).getClient();
        c.smembers(key);
        results.add(new FutureResult(c));
    }

    protected void srem(String key, String member) {
        Client c = jedis.getShard(key).getClient();
        c.srem(key, member);
        results.add(new FutureResult(c));
    }

    protected void spop(String key) {
        Client c = jedis.getShard(key).getClient();
        c.spop(key);
        results.add(new FutureResult(c));
    }

    protected void scard(String key) {
        Client c = jedis.getShard(key).getClient();
        c.scard(key);
        results.add(new FutureResult(c));
    }

    protected void sismember(String key, String member) {
        Client c = jedis.getShard(key).getClient();
        c.sismember(key, member);
        results.add(new FutureResult(c));
    }

    protected void srandmember(String key) {
        Client c = jedis.getShard(key).getClient();
        c.srandmember(key);
        results.add(new FutureResult(c));
    }

    protected void zadd(String key, double score, String member) {
        Client c = jedis.getShard(key).getClient();
        c.zadd(key, score, member);
        results.add(new FutureResult(c));
    }

    protected void zrange(String key, int start, int end) {
        Client c = jedis.getShard(key).getClient();
        c.zrange(key, start, end);
        results.add(new FutureResult(c));
    }

    protected void zrem(String key, String member) {
        Client c = jedis.getShard(key).getClient();
        c.zrem(key, member);
        results.add(new FutureResult(c));
    }

    protected void zincrby(String key, double score, String member) {
        Client c = jedis.getShard(key).getClient();
        c.zincrby(key, score, member);
        results.add(new FutureResult(c));
    }

    protected void zrevrange(String key, int start, int end) {
        Client c = jedis.getShard(key).getClient();
        c.zrevrange(key, start, end);
        results.add(new FutureResult(c));
    }

    protected void zrangeWithScores(String key, int start, int end) {
        Client c = jedis.getShard(key).getClient();
        c.zrangeWithScores(key, start, end);
        results.add(new FutureResult(c));
    }

    protected void zrevrangeWithScores(String key, int start, int end) {
        Client c = jedis.getShard(key).getClient();
        c.zrevrangeWithScores(key, start, end);
        results.add(new FutureResult(c));
    }

    protected void zcard(String key) {
        Client c = jedis.getShard(key).getClient();
        c.zcard(key);
        results.add(new FutureResult(c));
    }

    protected void zscore(String key, String member) {
        Client c = jedis.getShard(key).getClient();
        c.zscore(key, member);
        results.add(new FutureResult(c));
    }

    protected void sort(String key) {
        Client c = jedis.getShard(key).getClient();
        c.sort(key);
        results.add(new FutureResult(c));
    }

    protected void sort(String key, SortingParams sortingParameters) {
        Client c = jedis.getShard(key).getClient();
        c.sort(key, sortingParameters);
        results.add(new FutureResult(c));
    }

    protected void zcount(String key, double min, double max) {
        Client c = jedis.getShard(key).getClient();
        c.zcount(key, min, max);
        results.add(new FutureResult(c));
    }

    protected void zrangeByScore(String key, double min, double max) {
        Client c = jedis.getShard(key).getClient();
        c.zrangeByScore(key, min, max);
        results.add(new FutureResult(c));
    }

    protected void zrangeByScore(String key, double min, double max,
            int offset, int count) {
        Client c = jedis.getShard(key).getClient();
        c.zrangeByScore(key, min, max, offset, count);
        results.add(new FutureResult(c));
    }

    protected void zrangeByScoreWithScores(String key, double min, double max) {
        Client c = jedis.getShard(key).getClient();
        c.zrangeByScoreWithScores(key, min, max);
        results.add(new FutureResult(c));
    }

    protected void zrangeByScoreWithScores(String key, double min, double max,
            int offset, int count) {
        Client c = jedis.getShard(key).getClient();
        c.zrangeByScoreWithScores(key, min, max, offset, count);
        results.add(new FutureResult(c));
    }

    protected void zremrangeByScore(String key, double start, double end) {
        Client c = jedis.getShard(key).getClient();
        c.zremrangeByScore(key, start, end);
        results.add(new FutureResult(c));
    }

    public List<Object> getResults() {
        List<Object> r = new ArrayList<Object>();
        for (FutureResult fr : results) {
            r.add(fr.get());
        }
        return r;
    }

    public abstract void execute();
}