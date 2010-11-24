package redis.clients.jedis;

import redis.clients.util.SafeEncoder;

public class Client extends BinaryClient {
    public Client(final String host) {
        super(host);
    }

    public Client(final String host, final int port) {
        super(host, port);
    }

    public void set(final String key, final String value) {
        set(SafeEncoder.encode(key), SafeEncoder.encode(value));
    }

    public void get(final String key) {
        get(SafeEncoder.encode(key));
    }

    public void exists(final String key) {
        exists(SafeEncoder.encode(key));
    }

    public void del(final String... keys) {
        final byte[][] bkeys = new byte[keys.length][];
        for (int i = 0; i < keys.length; i++) {
            bkeys[i] = SafeEncoder.encode(keys[i]);
        }
        del(bkeys);
    }

    public void type(final String key) {
        type(SafeEncoder.encode(key));
    }

    public void keys(final String pattern) {
        keys(SafeEncoder.encode(pattern));
    }

    public void rename(final String oldkey, final String newkey) {
        rename(SafeEncoder.encode(oldkey), SafeEncoder.encode(newkey));
    }

    public void renamenx(final String oldkey, final String newkey) {
        renamenx(SafeEncoder.encode(oldkey), SafeEncoder.encode(newkey));
    }

    public void expire(final String key, final int seconds) {
        expire(SafeEncoder.encode(key), seconds);
    }

    public void expireAt(final String key, final long unixTime) {
        expireAt(SafeEncoder.encode(key), unixTime);
    }

    public void ttl(final String key) {
        ttl(SafeEncoder.encode(key));
    }

    public void move(final String key, final int dbIndex) {
        move(SafeEncoder.encode(key), dbIndex);
    }

    public void getSet(final String key, final String value) {
        getSet(SafeEncoder.encode(key), SafeEncoder.encode(value));
    }

    public void mget(final String... keys) {
        final byte[][] bkeys = new byte[keys.length][];
        for (int i = 0; i < bkeys.length; i++) {
            bkeys[i] = SafeEncoder.encode(keys[i]);
        }
        mget(bkeys);
    }

    public void setnx(final String key, final String value) {
        setnx(SafeEncoder.encode(key), SafeEncoder.encode(value));
    }

    public void mset(final String... keysvalues) {
        final byte[][] bkeysvalues = new byte[keysvalues.length][];
        for (int i = 0; i < keysvalues.length; i++) {
            bkeysvalues[i] = SafeEncoder.encode(keysvalues[i]);
        }
        mset(bkeysvalues);
    }

    public void msetnx(final String... keysvalues) {
        final byte[][] bkeysvalues = new byte[keysvalues.length][];
        for (int i = 0; i < keysvalues.length; i++) {
            bkeysvalues[i] = SafeEncoder.encode(keysvalues[i]);
        }
        msetnx(bkeysvalues);
    }

    public void decrBy(final String key, final int integer) {
        decrBy(SafeEncoder.encode(key), integer);
    }

    public void decr(final String key) {
        decr(SafeEncoder.encode(key));
    }

    public void incrBy(final String key, final int integer) {
        incrBy(SafeEncoder.encode(key), integer);
    }

    public void incr(final String key) {
        incr(SafeEncoder.encode(key));
    }

    public void rpush(final String key, final String string) {
        rpush(SafeEncoder.encode(key), SafeEncoder.encode(string));
    }

    public void lpush(final String key, final String string) {
        lpush(SafeEncoder.encode(key), SafeEncoder.encode(string));
    }

    public void llen(final String key) {
        llen(SafeEncoder.encode(key));
    }

    public void lrange(final String key, final int start, final int end) {
        lrange(SafeEncoder.encode(key), start, end);
    }

    public void ltrim(final String key, final int start, final int end) {
        ltrim(SafeEncoder.encode(key), start, end);
    }

    public void lindex(final String key, final int index) {
        lindex(SafeEncoder.encode(key), index);
    }

    public void lset(final String key, final int index, final String value) {
        lset(SafeEncoder.encode(key), index, SafeEncoder.encode(value));
    }

    public void lrem(final String key, int count, final String value) {
        lrem(SafeEncoder.encode(key), count, SafeEncoder.encode(value));
    }

    public void lpop(final String key) {
        lpop(SafeEncoder.encode(key));
    }

    public void rpop(final String key) {
        rpop(SafeEncoder.encode(key));
    }

    public void rpoplpush(final String srckey, final String dstkey) {
        rpoplpush(SafeEncoder.encode(srckey), SafeEncoder.encode(dstkey));
    }

    public void sadd(final String key, final String member) {
        sadd(SafeEncoder.encode(key), SafeEncoder.encode(member));
    }

    public void smembers(final String key) {
        smembers(SafeEncoder.encode(key));
    }

    public void srem(final String key, final String member) {
        srem(SafeEncoder.encode(key), SafeEncoder.encode(member));
    }

    public void spop(final String key) {
        spop(SafeEncoder.encode(key));
    }

    public void smove(final String srckey, final String dstkey,
            final String member) {
        smove(SafeEncoder.encode(srckey), SafeEncoder.encode(dstkey),
                SafeEncoder.encode(member));
    }

    public void scard(final String key) {
        scard(SafeEncoder.encode(key));
    }

    public void sismember(final String key, final String member) {
        sismember(SafeEncoder.encode(key), SafeEncoder.encode(member));
    }

    public void sinter(final String... keys) {
        final byte[][] bkeys = new byte[keys.length][];
        for (int i = 0; i < bkeys.length; i++) {
            bkeys[i] = SafeEncoder.encode(keys[i]);
        }
        sinter(bkeys);
    }

    public void sinterstore(final String dstkey, final String... keys) {
        final byte[][] bkeys = new byte[keys.length][];
        for (int i = 0; i < bkeys.length; i++) {
            bkeys[i] = SafeEncoder.encode(keys[i]);
        }
        sinterstore(SafeEncoder.encode(dstkey), bkeys);
    }

    public void sunion(final String... keys) {
        final byte[][] bkeys = new byte[keys.length][];
        for (int i = 0; i < bkeys.length; i++) {
            bkeys[i] = SafeEncoder.encode(keys[i]);
        }
        sunion(bkeys);
    }

    public void sunionstore(final String dstkey, final String... keys) {
        final byte[][] bkeys = new byte[keys.length][];
        for (int i = 0; i < bkeys.length; i++) {
            bkeys[i] = SafeEncoder.encode(keys[i]);
        }
        sunionstore(SafeEncoder.encode(dstkey), bkeys);
    }

    public void sdiff(final String... keys) {
        final byte[][] bkeys = new byte[keys.length][];
        for (int i = 0; i < bkeys.length; i++) {
            bkeys[i] = SafeEncoder.encode(keys[i]);
        }
        sdiff(bkeys);
    }

    public void sdiffstore(final String dstkey, final String... keys) {
        final byte[][] bkeys = new byte[keys.length][];
        for (int i = 0; i < bkeys.length; i++) {
            bkeys[i] = SafeEncoder.encode(keys[i]);
        }
        sdiffstore(SafeEncoder.encode(dstkey), bkeys);
    }

    public void srandmember(final String key) {
        srandmember(SafeEncoder.encode(key));
    }

    public void zadd(final String key, final double score, final String member) {
        zadd(SafeEncoder.encode(key), score, SafeEncoder.encode(member));
    }

    public void zrange(final String key, final int start, final int end) {
        zrange(SafeEncoder.encode(key), start, end);
    }

    public void zrem(final String key, final String member) {
        zrem(SafeEncoder.encode(key), SafeEncoder.encode(member));
    }

    public void zincrby(final String key, final double score,
            final String member) {
        zincrby(SafeEncoder.encode(key), score, SafeEncoder.encode(member));
    }

    public void zrevrange(final String key, final int start, final int end) {
        zrevrange(SafeEncoder.encode(key), start, end);
    }

    public void zrangeWithScores(final String key, final int start,
            final int end) {
        zrangeWithScores(SafeEncoder.encode(key), start, end);
    }

    public void zrevrangeWithScores(final String key, final int start,
            final int end) {
        zrevrangeWithScores(SafeEncoder.encode(key), start, end);
    }

    public void zcard(final String key) {
        zcard(SafeEncoder.encode(key));
    }

    public void zscore(final String key, final String member) {
        zscore(SafeEncoder.encode(key), SafeEncoder.encode(member));
    }

    public void watch(final String... keys) {
        final byte[][] bargs = new byte[keys.length][];
        for (int i = 0; i < bargs.length; i++) {
            bargs[i] = SafeEncoder.encode(keys[i]);
        }
        watch(bargs);
    }

    public void sort(final String key) {
        sort(SafeEncoder.encode(key));
    }

    public void sort(final String key, final SortingParams sortingParameters) {
        sort(SafeEncoder.encode(key), sortingParameters);
    }

    public void sort(final String key, final SortingParams sortingParameters,
            final String dstkey) {
        sort(SafeEncoder.encode(key), sortingParameters, SafeEncoder
                .encode(dstkey));
    }

    public void sort(final String key, final String dstkey) {
        sort(SafeEncoder.encode(key), SafeEncoder.encode(dstkey));
    }

    public void zcount(final String key, final double min, final double max) {
        zcount(SafeEncoder.encode(key), min, max);
    }

    public void zrangeByScore(final String key, final double min,
            final double max) {
        zrangeByScore(SafeEncoder.encode(key), min, max);
    }

    public void zrangeByScore(final String key, final String min,
            final String max) {
        zrangeByScore(SafeEncoder.encode(key), SafeEncoder.encode(min),
                SafeEncoder.encode(max));
    }

    public void zrangeByScore(final String key, final double min,
            final double max, final int offset, int count) {
        zrangeByScore(SafeEncoder.encode(key), min, max, offset, count);
    }

    public void zrangeByScoreWithScores(final String key, final double min,
            final double max) {
        zrangeByScoreWithScores(SafeEncoder.encode(key), min, max);
    }

    public void zrangeByScoreWithScores(final String key, final double min,
            final double max, final int offset, final int count) {
        zrangeByScoreWithScores(SafeEncoder.encode(key), min, max, offset,
                count);
    }

    public void zremrangeByScore(final String key, final double start,
            final double end) {
        zremrangeByScore(SafeEncoder.encode(key), start, end);
    }

    public void echo(final String string) {
        echo(SafeEncoder.encode(string));
    }
}