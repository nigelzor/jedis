package redis.clients.jedis.tests.commands;

import org.junit.Test;

import redis.clients.jedis.JedisException;
import redis.clients.jedis.JedisMonitor;

public class ControlCommandsTest extends JedisCommandTestBase {
    @Test
    public void save() {
	String status = jedis.save();
	assertEquals("OK", status);
    }

    @Test
    public void bgsave() {
	try {
	    String status = jedis.bgsave();
	    assertEquals("Background saving started", status);
	} catch (JedisException e) {
		assertTrue(
				"ERR Background save already in progress"
					.equalsIgnoreCase(e.getMessage()));
	}
    }

    @Test
    public void bgrewriteaof() {
	String status = jedis.bgrewriteaof();
	assertEquals("Background append only file rewriting started", status);
    }

    @Test
    public void lastsave() throws InterruptedException {
	long before = jedis.lastsave();
	String st = "";
	while (!st.equals("OK")) {
	    try {
		Thread.sleep(1000);
		st = jedis.save();
	    } catch (JedisException e) {

	    }
	}
	long after = jedis.lastsave();
	assertTrue((after - before) > 0);
    }

    @Test
    public void info() {
	String info = jedis.info();
	assertNotNull(info);
    }

    @Test
    public void monitor() {
	jedis.monitor(new JedisMonitor() {
	    @Override
		public void onCommand(String command) {
		assertTrue(command.contains("OK"));
		client.disconnect();
	    }
	});
    }
}