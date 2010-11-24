package redis.clients.jedis.tests.commands;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import redis.clients.jedis.JedisException;

public class ListCommandsTest extends JedisCommandTestBase {
    final byte[] bfoo = { 0x01, 0x02, 0x03, 0x04 };
    final byte[] bbar = { 0x05, 0x06, 0x07, 0x08 };
    final byte[] bcar = { 0x09, 0x0A, 0x0B, 0x0C };
    final byte[] bA = { 0x0A };
    final byte[] bB = { 0x0B };
    final byte[] bC = { 0x0C };
    final byte[] b1 = { 0x01 };
    final byte[] b2 = { 0x02 };
    final byte[] b3 = { 0x03 };
    final byte[] bhello = { 0x04, 0x02 };
    final byte[] bx = { 0x02, 0x04 };
    final byte[] bdst = { 0x11, 0x12, 0x13, 0x14 };

    @Test
    public void rpush() {
    	assertEquals("OK", jedis.rpush("foo", "bar"));
    	assertEquals("OK", jedis.rpush("foo", "foo"));

        // Binary
    	assertEquals("OK", jedis.rpush(bfoo, bbar));
    	assertEquals("OK", jedis.rpush(bfoo, bfoo));
    }

    @Test
    public void lpush() {
    	assertEquals("OK", jedis.lpush("foo", "bar"));
    	assertEquals("OK", jedis.lpush("foo", "foo"));

        // Binary
    	assertEquals("OK", jedis.lpush(bfoo, bbar));
    	assertEquals("OK", jedis.lpush(bfoo, bfoo));
    }

    @Test
    public void llen() {
        assertEquals(0, jedis.llen("foo").intValue());
        jedis.lpush("foo", "bar");
        jedis.lpush("foo", "car");
        assertEquals(2, jedis.llen("foo").intValue());

        // Binary
        assertEquals(0, jedis.llen(bfoo).intValue());
        jedis.lpush(bfoo, bbar);
        jedis.lpush(bfoo, bcar);
        assertEquals(2, jedis.llen(bfoo).intValue());

    }

    @Test
    public void llenNotOnList() {
        try {
            jedis.set("foo", "bar");
            jedis.llen("foo");
            fail("JedisException expected");
        } catch (final JedisException e) {
        }

        // Binary
        try {
            jedis.set(bfoo, bbar);
            jedis.llen(bfoo);
            fail("JedisException expected");
        } catch (final JedisException e) {
        }

    }

    @Test
    public void lrange() {
        jedis.rpush("foo", "a");
        jedis.rpush("foo", "b");
        jedis.rpush("foo", "c");

        List<String> expected = new ArrayList<String>();
        expected.add("a");
        expected.add("b");
        expected.add("c");

        List<String> range = jedis.lrange("foo", 0, 2);
        assertEquals(expected, range);

        range = jedis.lrange("foo", 0, 20);
        assertEquals(expected, range);

        expected = new ArrayList<String>();
        expected.add("b");
        expected.add("c");

        range = jedis.lrange("foo", 1, 2);
        assertEquals(expected, range);

        expected = new ArrayList<String>();
        range = jedis.lrange("foo", 2, 1);
        assertEquals(expected, range);

        // Binary
        jedis.rpush(bfoo, bA);
        jedis.rpush(bfoo, bB);
        jedis.rpush(bfoo, bC);

        List<byte[]> bexpected = new ArrayList<byte[]>();
        bexpected.add(bA);
        bexpected.add(bB);
        bexpected.add(bC);

        List<byte[]> brange = jedis.lrange(bfoo, 0, 2);
        assertEquals(bexpected, brange);

        brange = jedis.lrange(bfoo, 0, 20);
        assertEquals(bexpected, brange);

        bexpected = new ArrayList<byte[]>();
        bexpected.add(bB);
        bexpected.add(bC);

        brange = jedis.lrange(bfoo, 1, 2);
        assertEquals(bexpected, brange);

        bexpected = new ArrayList<byte[]>();
        brange = jedis.lrange(bfoo, 2, 1);
        assertEquals(bexpected, brange);

    }

    @Test
    public void ltrim() {
        jedis.lpush("foo", "1");
        jedis.lpush("foo", "2");
        jedis.lpush("foo", "3");
        String status = jedis.ltrim("foo", 0, 1);

        List<String> expected = new ArrayList<String>();
        expected.add("3");
        expected.add("2");

        assertEquals("OK", status);
        assertEquals(2, jedis.llen("foo").intValue());
        assertEquals(expected, jedis.lrange("foo", 0, 100));

        // Binary
        jedis.lpush(bfoo, b1);
        jedis.lpush(bfoo, b2);
        jedis.lpush(bfoo, b3);
        String bstatus = jedis.ltrim(bfoo, 0, 1);

        List<byte[]> bexpected = new ArrayList<byte[]>();
        bexpected.add(b3);
        bexpected.add(b2);

        assertEquals("OK", bstatus);
        assertEquals(2, jedis.llen(bfoo).intValue());
        assertEquals(bexpected, jedis.lrange(bfoo, 0, 100));

    }

    @Test
    public void lindex() {
        jedis.lpush("foo", "1");
        jedis.lpush("foo", "2");
        jedis.lpush("foo", "3");

        List<String> expected = new ArrayList<String>();
        expected.add("3");
        expected.add("bar");
        expected.add("1");

        String status = jedis.lset("foo", 1, "bar");

        assertEquals("OK", status);
        assertEquals(expected, jedis.lrange("foo", 0, 100));

        // Binary
        jedis.lpush(bfoo, b1);
        jedis.lpush(bfoo, b2);
        jedis.lpush(bfoo, b3);

        List<byte[]> bexpected = new ArrayList<byte[]>();
        bexpected.add(b3);
        bexpected.add(bbar);
        bexpected.add(b1);

        String bstatus = jedis.lset(bfoo, 1, bbar);

        assertEquals("OK", bstatus);
        assertEquals(bexpected, jedis.lrange(bfoo, 0, 100));
    }

    @Test
    public void lset() {
        jedis.lpush("foo", "1");
        jedis.lpush("foo", "2");
        jedis.lpush("foo", "3");

        assertEquals("3", jedis.lindex("foo", 0));
        assertEquals(null, jedis.lindex("foo", 100));

        // Binary
        jedis.lpush(bfoo, b1);
        jedis.lpush(bfoo, b2);
        jedis.lpush(bfoo, b3);

        assertArrayEquals(b3, jedis.lindex(bfoo, 0));
        assertEquals(null, jedis.lindex(bfoo, 100));

    }

    @Test
    public void lrem() {
        jedis.lpush("foo", "hello");
        jedis.lpush("foo", "hello");
        jedis.lpush("foo", "x");
        jedis.lpush("foo", "hello");
        jedis.lpush("foo", "c");
        jedis.lpush("foo", "b");
        jedis.lpush("foo", "a");

        long count = jedis.lrem("foo", -2, "hello");

        List<String> expected = new ArrayList<String>();
        expected.add("a");
        expected.add("b");
        expected.add("c");
        expected.add("hello");
        expected.add("x");

        assertEquals(2, count);
        assertEquals(expected, jedis.lrange("foo", 0, 1000));
        assertEquals(0, jedis.lrem("bar", 100, "foo").intValue());

        // Binary
        jedis.lpush(bfoo, bhello);
        jedis.lpush(bfoo, bhello);
        jedis.lpush(bfoo, bx);
        jedis.lpush(bfoo, bhello);
        jedis.lpush(bfoo, bC);
        jedis.lpush(bfoo, bB);
        jedis.lpush(bfoo, bA);

        long bcount = jedis.lrem(bfoo, -2, bhello);

        List<byte[]> bexpected = new ArrayList<byte[]>();
        bexpected.add(bA);
        bexpected.add(bB);
        bexpected.add(bC);
        bexpected.add(bhello);
        bexpected.add(bx);

        assertEquals(2, bcount);
        assertEquals(bexpected, jedis.lrange(bfoo, 0, 1000));
        assertEquals(0, jedis.lrem(bbar, 100, bfoo).intValue());

    }

    @Test
    public void lpop() {
        jedis.rpush("foo", "a");
        jedis.rpush("foo", "b");
        jedis.rpush("foo", "c");

        String element = jedis.lpop("foo");
        assertEquals("a", element);

        List<String> expected = new ArrayList<String>();
        expected.add("b");
        expected.add("c");

        assertEquals(expected, jedis.lrange("foo", 0, 1000));
        jedis.lpop("foo");
        jedis.lpop("foo");

        element = jedis.lpop("foo");
        assertEquals(null, element);

        // Binary
        jedis.rpush(bfoo, bA);
        jedis.rpush(bfoo, bB);
        jedis.rpush(bfoo, bC);

        byte[] belement = jedis.lpop(bfoo);
        assertArrayEquals(bA, belement);

        List<byte[]> bexpected = new ArrayList<byte[]>();
        bexpected.add(bB);
        bexpected.add(bC);

        assertEquals(bexpected, jedis.lrange(bfoo, 0, 1000));
        jedis.lpop(bfoo);
        jedis.lpop(bfoo);

        belement = jedis.lpop(bfoo);
        assertEquals(null, belement);

    }

    @Test
    public void rpop() {
        jedis.rpush("foo", "a");
        jedis.rpush("foo", "b");
        jedis.rpush("foo", "c");

        String element = jedis.rpop("foo");
        assertEquals("c", element);

        List<String> expected = new ArrayList<String>();
        expected.add("a");
        expected.add("b");

        assertEquals(expected, jedis.lrange("foo", 0, 1000));
        jedis.rpop("foo");
        jedis.rpop("foo");

        element = jedis.rpop("foo");
        assertEquals(null, element);

        // Binary
        jedis.rpush(bfoo, bA);
        jedis.rpush(bfoo, bB);
        jedis.rpush(bfoo, bC);

        byte[] belement = jedis.rpop(bfoo);
        assertArrayEquals(bC, belement);

        List<byte[]> bexpected = new ArrayList<byte[]>();
        bexpected.add(bA);
        bexpected.add(bB);

        assertEquals(bexpected, jedis.lrange(bfoo, 0, 1000));
        jedis.rpop(bfoo);
        jedis.rpop(bfoo);

        belement = jedis.rpop(bfoo);
        assertEquals(null, belement);

    }

    @Test
    public void rpoplpush() {
        jedis.rpush("foo", "a");
        jedis.rpush("foo", "b");
        jedis.rpush("foo", "c");

        jedis.rpush("dst", "foo");
        jedis.rpush("dst", "bar");

        String element = jedis.rpoplpush("foo", "dst");

        assertEquals("c", element);

        List<String> srcExpected = new ArrayList<String>();
        srcExpected.add("a");
        srcExpected.add("b");

        List<String> dstExpected = new ArrayList<String>();
        dstExpected.add("c");
        dstExpected.add("foo");
        dstExpected.add("bar");

        assertEquals(srcExpected, jedis.lrange("foo", 0, 1000));
        assertEquals(dstExpected, jedis.lrange("dst", 0, 1000));

        // Binary
        jedis.rpush(bfoo, bA);
        jedis.rpush(bfoo, bB);
        jedis.rpush(bfoo, bC);

        jedis.rpush(bdst, bfoo);
        jedis.rpush(bdst, bbar);

        byte[] belement = jedis.rpoplpush(bfoo, bdst);

        assertArrayEquals(bC, belement);

        List<byte[]> bsrcExpected = new ArrayList<byte[]>();
        bsrcExpected.add(bA);
        bsrcExpected.add(bB);

        List<byte[]> bdstExpected = new ArrayList<byte[]>();
        bdstExpected.add(bC);
        bdstExpected.add(bfoo);
        bdstExpected.add(bbar);

        assertEquals(bsrcExpected, jedis.lrange(bfoo, 0, 1000));
        assertEquals(bdstExpected, jedis.lrange(bdst, 0, 1000));

    }
}