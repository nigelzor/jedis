package redis.clients.jedis.tests.commands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Protocol.Keyword;

public class BinaryValuesCommandsTest extends JedisCommandTestBase {
    byte[] bfoo = { 0x01, 0x02, 0x03, 0x04 };
    byte[] bbar = { 0x05, 0x06, 0x07, 0x08 };
    byte[] binaryValue;

    @Before
    public void startUp() {
        StringBuilder sb = new StringBuilder();

        for (int n = 0; n < 1000; n++) {
            sb.append("A");
        }

        binaryValue = sb.toString().getBytes();
    }

    @Test
    public void setAndGet() {
        String status = jedis.set(bfoo, binaryValue);
        assertTrue(Keyword.OK.name().equalsIgnoreCase(status));

        byte[] value = jedis.get(bfoo);
        assertTrue(Arrays.equals(binaryValue, value));

        assertNull(jedis.get(bbar));
    }

    @Test
    public void getSet() {
        byte[] value = jedis.getSet(bfoo, binaryValue);
        assertNull(value);
        value = jedis.get(bfoo);
        assertTrue(Arrays.equals(binaryValue, value));
    }

    @Test
    public void mget() {
        List<byte[]> values = jedis.mget(bfoo, bbar);
        List<byte[]> expected = new ArrayList<byte[]>();
        expected.add(null);
        expected.add(null);

        assertEquals(expected, values);

        jedis.set(bfoo, binaryValue);

        expected = new ArrayList<byte[]>();
        expected.add(binaryValue);
        expected.add(null);
        values = jedis.mget(bfoo, bbar);

        assertEquals(expected, values);

        jedis.set(bbar, bfoo);

        expected = new ArrayList<byte[]>();
        expected.add(binaryValue);
        expected.add(bfoo);
        values = jedis.mget(bfoo, bbar);

        assertEquals(expected, values);
    }

    @Test
    public void setnx() {
        long status = jedis.setnx(bfoo, binaryValue);
        assertEquals(1, status);
        assertTrue(Arrays.equals(binaryValue, jedis.get(bfoo)));

        status = jedis.setnx(bfoo, bbar);
        assertEquals(0, status);
        assertTrue(Arrays.equals(binaryValue, jedis.get(bfoo)));
    }

    @Test
    public void mset() {
        String status = jedis.mset(bfoo, binaryValue, bbar, bfoo);
        assertEquals(Keyword.OK.name(), status);
        assertTrue(Arrays.equals(binaryValue, jedis.get(bfoo)));
        assertTrue(Arrays.equals(bfoo, jedis.get(bbar)));
    }

    @Test
    public void msetnx() {
        long status = jedis.msetnx(bfoo, binaryValue, bbar, bfoo);
        assertEquals(1, status);
        assertTrue(Arrays.equals(binaryValue, jedis.get(bfoo)));
        assertTrue(Arrays.equals(bfoo, jedis.get(bbar)));

        status = jedis.msetnx(bfoo, bbar, "bar2".getBytes(), "foo2".getBytes());
        assertEquals(0, status);
        assertTrue(Arrays.equals(binaryValue, jedis.get(bfoo)));
        assertTrue(Arrays.equals(bfoo, jedis.get(bbar)));
    }

    @Test
    public void incrWrongValue() {
        jedis.set(bfoo, binaryValue);
        long value = jedis.incr(bfoo);
        assertEquals(1, value);
    }

    @Test
    public void incr() {
        long value = jedis.incr(bfoo);
        assertEquals(1, value);
        value = jedis.incr(bfoo);
        assertEquals(2, value);
    }

    @Test
    public void incrByWrongValue() {
        jedis.set(bfoo, binaryValue);
        long value = jedis.incrBy(bfoo, 2);
        assertEquals(2, value);
    }

    @Test
    public void incrBy() {
        long value = jedis.incrBy(bfoo, 2);
        assertEquals(2, value);
        value = jedis.incrBy(bfoo, 2);
        assertEquals(4, value);
    }

    @Test
    public void decrWrongValue() {
        jedis.set(bfoo, binaryValue);
        long value = jedis.decr(bfoo);
        assertEquals(-1, value);
    }

    @Test
    public void decr() {
        long value = jedis.decr(bfoo);
        assertEquals(-1, value);
        value = jedis.decr(bfoo);
        assertEquals(-2, value);
    }

    @Test
    public void decrByWrongValue() {
        jedis.set(bfoo, binaryValue);
        long value = jedis.decrBy(bfoo, 2);
        assertEquals(-2, value);
    }

    @Test
    public void decrBy() {
        long value = jedis.decrBy(bfoo, 2);
        assertEquals(-2, value);
        value = jedis.decrBy(bfoo, 2);
        assertEquals(-4, value);
    }
}