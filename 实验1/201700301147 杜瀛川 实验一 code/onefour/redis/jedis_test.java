package redis;

import java.util.Map;
import redis.clients.jedis.Jedis;

public class jedis_test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Jedis jedis = new Jedis("localhost");
		jedis.hset("student.scofield", "English", "45");
		jedis.hset("student.scofield", "Math", "89");
		jedis.hset("student.scofield", "Computer", "100");
		Map<String, String> value = jedis.hgetAll("student.scofield");
		for (Map.Entry<String, String> entry : value.entrySet()) {
			System.out.println(entry.getKey() + ":" + entry.getValue());
		}
	}
}
