package redis.clients.jedis;

import static redis.clients.jedis.Protocol.toByteArray;
import static redis.clients.jedis.Protocol.Command.*;
import static redis.clients.jedis.Protocol.Keyword.LIMIT;
import static redis.clients.jedis.Protocol.Keyword.NO;
import static redis.clients.jedis.Protocol.Keyword.ONE;
import static redis.clients.jedis.Protocol.Keyword.STORE;
import static redis.clients.jedis.Protocol.Keyword.WITHSCORES;

import java.util.ArrayList;
import java.util.List;
import redis.clients.jedis.Protocol.Command;
import redis.clients.util.SafeEncoder;

public class BinaryClient extends Connection {
    public enum LIST_POSITION {
        BEFORE, AFTER;
        public final byte[] raw;

        private LIST_POSITION() {
            raw = SafeEncoder.encode(name());
        }
    }

    private boolean isInMulti;

    public boolean isInMulti() {
        return isInMulti;
    }

    public BinaryClient(final String host) {
        super(host);
    }

    public BinaryClient(final String host, final int port) {
        super(host, port);
    }

    public void ping() {
        sendCommand(PING);
    }

    public void set(final byte[] key, final byte[] value) {
        sendCommand(Command.SET, key, value);
    }

    public void get(final byte[] key) {
        sendCommand(Command.GET, key);
    }

    public void quit() {
        sendCommand(QUIT);
    }

    public void exists(final byte[] key) {
        sendCommand(EXISTS, key);
    }

    public void del(final byte[]... keys) {
        sendCommand(DEL, keys);
    }

    public void type(final byte[] key) {
        sendCommand(TYPE, key);
    }

    public void flushDB() {
        sendCommand(FLUSHDB);
    }

    public void keys(final byte[] pattern) {
        sendCommand(KEYS, pattern);
    }

    public void randomKey() {
        sendCommand(RANDOMKEY);
    }

    public void rename(final byte[] oldkey, final byte[] newkey) {
        sendCommand(RENAME, oldkey, newkey);
    }

    public void renamenx(final byte[] oldkey, final byte[] newkey) {
        sendCommand(RENAMENX, oldkey, newkey);
    }

    public void dbSize() {
        sendCommand(DBSIZE);
    }

    public void expire(final byte[] key, final int seconds) {
        sendCommand(EXPIRE, key, toByteArray(seconds));
    }

    public void expireAt(final byte[] key, final long unixTime) {
        sendCommand(EXPIREAT, key, toByteArray(unixTime));
    }

    public void ttl(final byte[] key) {
        sendCommand(TTL, key);
    }

    public void select(final int index) {
        sendCommand(SELECT, toByteArray(index));
    }

    public void move(final byte[] key, final int dbIndex) {
        sendCommand(MOVE, key, toByteArray(dbIndex));
    }

    public void flushAll() {
        sendCommand(FLUSHALL);
    }

    public void getSet(final byte[] key, final byte[] value) {
        sendCommand(GETSET, key, value);
    }

    public void mget(final byte[]... keys) {
        sendCommand(MGET, keys);
    }

    public void setnx(final byte[] key, final byte[] value) {
        sendCommand(SETNX, key, value);
    }

    public void mset(final byte[]... keysvalues) {
        sendCommand(MSET, keysvalues);
    }

    public void msetnx(final byte[]... keysvalues) {
        sendCommand(MSETNX, keysvalues);
    }

    public void decrBy(final byte[] key, final int integer) {
        sendCommand(DECRBY, key, toByteArray(integer));
    }

    public void decr(final byte[] key) {
        sendCommand(DECR, key);
    }

    public void incrBy(final byte[] key, final int integer) {
        sendCommand(INCRBY, key, toByteArray(integer));
    }

    public void incr(final byte[] key) {
        sendCommand(INCR, key);
    }

    public void rpush(final byte[] key, final byte[] string) {
        sendCommand(RPUSH, key, string);
    }

    public void lpush(final byte[] key, final byte[] string) {
        sendCommand(LPUSH, key, string);
    }

    public void llen(final byte[] key) {
        sendCommand(LLEN, key);
    }

    public void lrange(final byte[] key, final int start, final int end) {
        sendCommand(LRANGE, key, toByteArray(start), toByteArray(end));
    }

    public void ltrim(final byte[] key, final int start, final int end) {
        sendCommand(LTRIM, key, toByteArray(start), toByteArray(end));
    }

    public void lindex(final byte[] key, final int index) {
        sendCommand(LINDEX, key, toByteArray(index));
    }

    public void lset(final byte[] key, final int index, final byte[] value) {
        sendCommand(LSET, key, toByteArray(index), value);
    }

    public void lrem(final byte[] key, int count, final byte[] value) {
        sendCommand(LREM, key, toByteArray(count), value);
    }

    public void lpop(final byte[] key) {
        sendCommand(LPOP, key);
    }

    public void rpop(final byte[] key) {
        sendCommand(RPOP, key);
    }

    public void rpoplpush(final byte[] srckey, final byte[] dstkey) {
        sendCommand(RPOPLPUSH, srckey, dstkey);
    }

    public void sadd(final byte[] key, final byte[] member) {
        sendCommand(SADD, key, member);
    }

    public void smembers(final byte[] key) {
        sendCommand(SMEMBERS, key);
    }

    public void srem(final byte[] key, final byte[] member) {
        sendCommand(SREM, key, member);
    }

    public void spop(final byte[] key) {
        sendCommand(SPOP, key);
    }

    public void smove(final byte[] srckey, final byte[] dstkey,
            final byte[] member) {
        sendCommand(SMOVE, srckey, dstkey, member);
    }

    public void scard(final byte[] key) {
        sendCommand(SCARD, key);
    }

    public void sismember(final byte[] key, final byte[] member) {
        sendCommand(SISMEMBER, key, member);
    }

    public void sinter(final byte[]... keys) {
        sendCommand(SINTER, keys);
    }

    public void sinterstore(final byte[] dstkey, final byte[]... keys) {
        final byte[][] params = new byte[keys.length + 1][];
        params[0] = dstkey;
        System.arraycopy(keys, 0, params, 1, keys.length);
        sendCommand(SINTERSTORE, params);
    }

    public void sunion(final byte[]... keys) {
        sendCommand(SUNION, keys);
    }

    public void sunionstore(final byte[] dstkey, final byte[]... keys) {
        byte[][] params = new byte[keys.length + 1][];
        params[0] = dstkey;
        System.arraycopy(keys, 0, params, 1, keys.length);
        sendCommand(SUNIONSTORE, params);
    }

    public void sdiff(final byte[]... keys) {
        sendCommand(SDIFF, keys);
    }

    public void sdiffstore(final byte[] dstkey, final byte[]... keys) {
        byte[][] params = new byte[keys.length + 1][];
        params[0] = dstkey;
        System.arraycopy(keys, 0, params, 1, keys.length);
        sendCommand(SDIFFSTORE, params);
    }

    public void srandmember(final byte[] key) {
        sendCommand(SRANDMEMBER, key);
    }

    public void zadd(final byte[] key, final double score, final byte[] member) {
        sendCommand(ZADD, key, toByteArray(score), member);
    }

    public void zrange(final byte[] key, final int start, final int end) {
        sendCommand(ZRANGE, key, toByteArray(start), toByteArray(end));
    }

    public void zrem(final byte[] key, final byte[] member) {
        sendCommand(ZREM, key, member);
    }

    public void zincrby(final byte[] key, final double score,
            final byte[] member) {
        sendCommand(ZINCRBY, key, toByteArray(score), member);
    }

    public void zrevrange(final byte[] key, final int start, final int end) {
        sendCommand(ZREVRANGE, key, toByteArray(start), toByteArray(end));
    }

    public void zrangeWithScores(final byte[] key, final int start,
            final int end) {
        sendCommand(ZRANGE, key, toByteArray(start), toByteArray(end),
                WITHSCORES.raw);
    }

    public void zrevrangeWithScores(final byte[] key, final int start,
            final int end) {
        sendCommand(ZREVRANGE, key, toByteArray(start), toByteArray(end),
                WITHSCORES.raw);
    }

    public void zcard(final byte[] key) {
        sendCommand(ZCARD, key);
    }

    public void zscore(final byte[] key, final byte[] member) {
        sendCommand(ZSCORE, key, member);
    }

    public void watch(final byte[]... keys) {
        sendCommand(WATCH, keys);
    }

    public void unwatch() {
        sendCommand(UNWATCH);
    }

    public void sort(final byte[] key) {
        sendCommand(SORT, key);
    }

    public void sort(final byte[] key, final SortingParams sortingParameters) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(key);
        args.addAll(sortingParameters.getParams());
        sendCommand(SORT, args.toArray(new byte[args.size()][]));
    }

    public void sort(final byte[] key, final SortingParams sortingParameters,
            final byte[] dstkey) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(key);
        args.addAll(sortingParameters.getParams());
        args.add(STORE.raw);
        args.add(dstkey);
        sendCommand(SORT, args.toArray(new byte[args.size()][]));
    }

    public void sort(final byte[] key, final byte[] dstkey) {
        sendCommand(SORT, key, STORE.raw, dstkey);
    }

    public void auth(final String password) {
        sendCommand(AUTH, password);
    }

    public void zcount(final byte[] key, final double min, final double max) {
        sendCommand(ZCOUNT, key, toByteArray(min), toByteArray(max));
    }

    public void zrangeByScore(final byte[] key, final double min,
            final double max) {
        sendCommand(ZRANGEBYSCORE, key, toByteArray(min), toByteArray(max));
    }

    public void zrangeByScore(final byte[] key, final byte[] min,
            final byte[] max) {
        sendCommand(ZRANGEBYSCORE, key, min, max);
    }

    public void zrangeByScore(final byte[] key, final double min,
            final double max, final int offset, int count) {
        sendCommand(ZRANGEBYSCORE, key, toByteArray(min), toByteArray(max),
                LIMIT.raw, toByteArray(offset), toByteArray(count));
    }

    public void zrangeByScoreWithScores(final byte[] key, final double min,
            final double max) {
        sendCommand(ZRANGEBYSCORE, key, toByteArray(min), toByteArray(max),
                WITHSCORES.raw);
    }

    public void zrangeByScoreWithScores(final byte[] key, final double min,
            final double max, final int offset, final int count) {
        sendCommand(ZRANGEBYSCORE, key, toByteArray(min), toByteArray(max),
                LIMIT.raw, toByteArray(offset), toByteArray(count),
                WITHSCORES.raw);
    }

    public void zremrangeByScore(final byte[] key, final double start,
            final double end) {
        sendCommand(ZREMRANGEBYSCORE, key, toByteArray(start), toByteArray(end));
    }

    public void save() {
        sendCommand(SAVE);
    }

    public void bgsave() {
        sendCommand(BGSAVE);
    }

    public void bgrewriteaof() {
        sendCommand(BGREWRITEAOF);
    }

    public void lastsave() {
        sendCommand(LASTSAVE);
    }

    public void shutdown() {
        sendCommand(SHUTDOWN);
    }

    public void info() {
        sendCommand(INFO);
    }

    public void monitor() {
        sendCommand(MONITOR);
    }

    public void slaveof(final String host, final int port) {
        sendCommand(SLAVEOF, host, String.valueOf(port));
    }

    public void slaveofNoOne() {
        sendCommand(SLAVEOF, NO.raw, ONE.raw);
    }

    public void sync() {
        sendCommand(SYNC);
    }

    public void echo(final byte[] string) {
        sendCommand(ECHO, string);
    }
}