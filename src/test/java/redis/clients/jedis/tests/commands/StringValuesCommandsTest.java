package redis.clients.jedis.tests.commands;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class StringValuesCommandsTest extends JedisCommandTestBase {
	@Test
	public void setAndGet() {
		String status = jedis.set("foo", "bar");
		assertEquals("OK", status);

		String value = jedis.get("foo");
		assertEquals("bar", value);

		assertEquals(null, jedis.get("bar"));
	}

	@Test
	public void getSet() {
		String value = jedis.getSet("foo", "bar");
		assertEquals(null, value);
		value = jedis.get("foo");
		assertEquals("bar", value);
	}

	@Test
	public void mget() {
		List<String> values = jedis.mget("foo", "bar");
		List<String> expected = new ArrayList<String>();
		expected.add(null);
		expected.add(null);

		assertEquals(expected, values);

		jedis.set("foo", "bar");

		expected = new ArrayList<String>();
		expected.add("bar");
		expected.add(null);
		values = jedis.mget("foo", "bar");

		assertEquals(expected, values);

		jedis.set("bar", "foo");

		expected = new ArrayList<String>();
		expected.add("bar");
		expected.add("foo");
		values = jedis.mget("foo", "bar");

		assertEquals(expected, values);
	}

	@Test
	public void setnx() {
		int status = jedis.setnx("foo", "bar");
		assertEquals(1, status);
		assertEquals("bar", jedis.get("foo"));

		status = jedis.setnx("foo", "bar2");
		assertEquals(0, status);
		assertEquals("bar", jedis.get("foo"));
	}

	@Test
	public void mset() {
		String status = jedis.mset("foo", "bar", "bar", "foo");
		assertEquals("OK", status);
		assertEquals("bar", jedis.get("foo"));
		assertEquals("foo", jedis.get("bar"));
	}

	@Test
	public void msetnx() {
		int status = jedis.msetnx("foo", "bar", "bar", "foo");
		assertEquals(1, status);
		assertEquals("bar", jedis.get("foo"));
		assertEquals("foo", jedis.get("bar"));

		status = jedis.msetnx("foo", "bar1", "bar2", "foo2");
		assertEquals(0, status);
		assertEquals("bar", jedis.get("foo"));
		assertEquals("foo", jedis.get("bar"));
	}

	@Test
	public void incrWrongValue() {
		jedis.set("foo", "bar");
		assertEquals((Integer) 1, jedis.incr("foo"));
	}

	@Test
	public void incr() {
		int value = jedis.incr("foo");
		assertEquals(1, value);
		assertEquals((Integer) 2, jedis.incr("foo"));
	}

	@Test
	public void incrByWrongValue() {
		jedis.set("foo", "bar");
		assertEquals((Integer) 2, jedis.incrBy("foo", 2));
	}

	@Test
	public void incrBy() {
		int value = jedis.incrBy("foo", 2);
		assertEquals(2, value);
		value = jedis.incrBy("foo", 2);
		assertEquals(4, value);
	}

	@Test
	public void decrWrongValue() {
		jedis.set("foo", "bar");
		assertEquals((Integer) (-1), jedis.decr("foo"));
	}

	@Test
	public void decr() {
		int value = jedis.decr("foo");
		assertEquals(-1, value);
		value = jedis.decr("foo");
		assertEquals(-2, value);
	}

	@Test
	public void decrByWrongValue() {
		jedis.set("foo", "bar");
		assertEquals((Integer) (-2), jedis.decrBy("foo", 2));
	}

	@Test
	public void decrBy() {
		int value = jedis.decrBy("foo", 2);
		assertEquals(-2, value);
		value = jedis.decrBy("foo", 2);
		assertEquals(-4, value);
	}
}