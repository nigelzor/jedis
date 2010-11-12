package redis.clients.jedis;

import java.util.ArrayList;
import java.util.List;

public class Client extends Connection {
    public enum LIST_POSITION {
        BEFORE, AFTER
    }

    public Client(String host) {
        super(host);
    }

    public Client(String host, int port) {
        super(host, port);
    }

    public void ping() {
        sendCommand("PING");
    }

    public void set(String key, String value) {
        sendCommand("SET", key, value);
    }

    public void get(String key) {
        sendCommand("GET", key);
    }

    public void quit() {
        sendCommand("QUIT");
    }

    public void exists(String key) {
        sendCommand("EXISTS", key);
    }

    public void del(String... keys) {
        sendCommand("DEL", keys);
    }

    public void type(String key) {
        sendCommand("TYPE", key);
    }

    public void flushDB() {
        sendCommand("FLUSHDB");
    }

    public void keys(String pattern) {
        sendCommand("KEYS", pattern);
    }

    public void randomKey() {
        sendCommand("RANDOMKEY");
    }

    public void rename(String oldkey, String newkey) {
        sendCommand("RENAME", oldkey, newkey);
    }

    public void renamenx(String oldkey, String newkey) {
        sendCommand("RENAMENX", oldkey, newkey);
    }

    public void dbSize() {
        sendCommand("DBSIZE");
    }

    public void expire(String key, int seconds) {
        sendCommand("EXPIRE", key, String.valueOf(seconds));
    }

    public void expireAt(String key, long unixTime) {
        sendCommand("EXPIREAT", key, String.valueOf(unixTime));
    }

    public void ttl(String key) {
        sendCommand("TTL", key);
    }

    public void select(int index) {
        sendCommand("SELECT", String.valueOf(index));
    }

    public void move(String key, int dbIndex) {
        sendCommand("MOVE", key, String.valueOf(dbIndex));
    }

    public void flushAll() {
        sendCommand("FLUSHALL");
    }

    public void getSet(String key, String value) {
        sendCommand("GETSET", key, value);
    }

    public void mget(String... keys) {
        sendCommand("MGET", keys);
    }

    public void setnx(String key, String value) {
        sendCommand("SETNX", key, value);
    }

    public void mset(String... keysvalues) {
        sendCommand("MSET", keysvalues);
    }

    public void msetnx(String... keysvalues) {
        sendCommand("MSETNX", keysvalues);
    }

    public void decrBy(String key, int integer) {
        sendCommand("DECRBY", key, String.valueOf(integer));
    }

    public void decr(String key) {
        sendCommand("DECR", key);
    }

    public void incrBy(String key, int integer) {
        sendCommand("INCRBY", key, String.valueOf(integer));
    }

    public void incr(String key) {
        sendCommand("INCR", key);
    }

    public void rpush(String key, String string) {
        sendCommand("RPUSH", key, string);
    }

    public void lpush(String key, String string) {
        sendCommand("LPUSH", key, string);
    }

    public void llen(String key) {
        sendCommand("LLEN", key);
    }

    public void lrange(String key, int start, int end) {
        sendCommand("LRANGE", key, String.valueOf(start), String.valueOf(end));
    }

    public void ltrim(String key, int start, int end) {
        sendCommand("LTRIM", key, String.valueOf(start), String.valueOf(end));
    }

    public void lindex(String key, int index) {
        sendCommand("LINDEX", key, String.valueOf(index));
    }

    public void lset(String key, int index, String value) {
        sendCommand("LSET", key, String.valueOf(index), value);
    }

    public void lrem(String key, int count, String value) {
        sendCommand("LREM", key, String.valueOf(count), value);
    }

    public void lpop(String key) {
        sendCommand("LPOP", key);
    }

    public void rpop(String key) {
        sendCommand("RPOP", key);
    }

    public void rpoplpush(String srckey, String dstkey) {
        sendCommand("RPOPLPUSH", srckey, dstkey);
    }

    public void sadd(String key, String member) {
        sendCommand("SADD", key, member);
    }

    public void smembers(String key) {
        sendCommand("SMEMBERS", key);
    }

    public void srem(String key, String member) {
        sendCommand("SREM", key, member);
    }

    public void spop(String key) {
        sendCommand("SPOP", key);
    }

    public void smove(String srckey, String dstkey, String member) {
        sendCommand("SMOVE", srckey, dstkey, member);
    }

    public void scard(String key) {
        sendCommand("SCARD", key);
    }

    public void sismember(String key, String member) {
        sendCommand("SISMEMBER", key, member);
    }

    public void sinter(String... keys) {
        sendCommand("SINTER", keys);
    }

    public void sinterstore(String dstkey, String... keys) {
        String[] params = new String[keys.length + 1];
        params[0] = dstkey;
        System.arraycopy(keys, 0, params, 1, keys.length);
        sendCommand("SINTERSTORE", params);
    }

    public void sunion(String... keys) {
        sendCommand("SUNION", keys);
    }

    public void sunionstore(String dstkey, String... keys) {
        String[] params = new String[keys.length + 1];
        params[0] = dstkey;
        System.arraycopy(keys, 0, params, 1, keys.length);
        sendCommand("SUNIONSTORE", params);
    }

    public void sdiff(String... keys) {
        sendCommand("SDIFF", keys);
    }

    public void sdiffstore(String dstkey, String... keys) {
        String[] params = new String[keys.length + 1];
        params[0] = dstkey;
        System.arraycopy(keys, 0, params, 1, keys.length);
        sendCommand("SDIFFSTORE", params);
    }

    public void srandmember(String key) {
        sendCommand("SRANDMEMBER", key);
    }

    public void zadd(String key, double score, String member) {
        sendCommand("ZADD", key, String.valueOf(score), member);
    }

    public void zrange(String key, int start, int end) {
        sendCommand("ZRANGE", key, String.valueOf(start), String.valueOf(end));
    }

    public void zrem(String key, String member) {
        sendCommand("ZREM", key, member);
    }

    public void zincrby(String key, double score, String member) {
        sendCommand("ZINCRBY", key, String.valueOf(score), member);
    }

    public void zrevrange(String key, int start, int end) {
        sendCommand("ZREVRANGE", key, String.valueOf(start), String
                .valueOf(end));
    }

    public void zrangeWithScores(String key, int start, int end) {
        sendCommand("ZRANGE", key, String.valueOf(start), String.valueOf(end),
                "WITHSCORES");
    }

    public void zrevrangeWithScores(String key, int start, int end) {
        sendCommand("ZREVRANGE", key, String.valueOf(start), String
                .valueOf(end), "WITHSCORES");
    }

    public void zcard(String key) {
        sendCommand("ZCARD", key);
    }

    public void zscore(String key, String member) {
        sendCommand("ZSCORE", key, member);
    }

    public void watch(String key) {
        sendCommand("WATCH", key);
    }

    public void unwatch() {
        sendCommand("UNWATCH");
    }

    public void sort(String key) {
        sendCommand("SORT", key);
    }

    public void sort(String key, SortingParams sortingParameters) {
        List<String> args = new ArrayList<String>();
        args.add(key);
        args.addAll(sortingParameters.getParams());
        sendCommand("SORT", args.toArray(new String[args.size()]));
    }

    public void sort(String key, SortingParams sortingParameters, String dstkey) {
        List<String> args = new ArrayList<String>();
        args.add(key);
        args.addAll(sortingParameters.getParams());
        args.add("STORE");
        args.add(dstkey);
        sendCommand("SORT", args.toArray(new String[args.size()]));
    }

    public void sort(String key, String dstkey) {
        sendCommand("SORT", key, "STORE", dstkey);
    }

    public void auth(String password) {
        sendCommand("AUTH", password);
    }

    public void zcount(String key, double min, double max) {
        sendCommand("ZCOUNT", key, String.valueOf(min), String.valueOf(max));
    }

    public void zrangeByScore(String key, double min, double max) {
        sendCommand("ZRANGEBYSCORE", key, String.valueOf(min), String
                .valueOf(max));
    }

    public void zrangeByScore(String key, String min, String max) {
        sendCommand("ZRANGEBYSCORE", key, min, max);
    }

    public void zrangeByScore(String key, double min, double max, int offset,
            int count) {
        sendCommand("ZRANGEBYSCORE", key, String.valueOf(min), String
                .valueOf(max), "LIMIT", String.valueOf(offset), String
                .valueOf(count));
    }

    public void zrangeByScoreWithScores(String key, double min, double max) {
        sendCommand("ZRANGEBYSCORE", key, String.valueOf(min), String
                .valueOf(max), "WITHSCORES");
    }

    public void zrangeByScoreWithScores(String key, double min, double max,
            int offset, int count) {
        sendCommand("ZRANGEBYSCORE", key, String.valueOf(min), String
                .valueOf(max), "LIMIT", String.valueOf(offset), String
                .valueOf(count), "WITHSCORES");
    }

    public void zremrangeByScore(String key, double start, double end) {
        sendCommand("ZREMRANGEBYSCORE", key, String.valueOf(start), String
                .valueOf(end));
    }

    public void save() {
        sendCommand("SAVE");
    }

    public void bgsave() {
        sendCommand("BGSAVE");
    }

    public void bgrewriteaof() {
        sendCommand("BGREWRITEAOF");
    }

    public void lastsave() {
        sendCommand("LASTSAVE");
    }

    public void shutdown() {
        sendCommand("SHUTDOWN");
    }

    public void info() {
        sendCommand("INFO");
    }

    public void monitor() {
        sendCommand("MONITOR");
    }

    public void slaveof(String host, int port) {
        sendCommand("SLAVEOF", host, String.valueOf(port));
    }

    public void slaveofNoOne() {
        sendCommand("SLAVEOF", "no", "one");
    }

    public void sync() {
        sendCommand("SYNC");
    }

    public void echo(String string) {
        sendCommand("ECHO", string);
    }
}