package storm.drpc;

import backtype.storm.utils.DRPCClient;

public class DrpcRemoteClient {

	public static void main(String[] args) {
		DRPCClient client = new DRPCClient("192.168.80.100", 3772);
		String result = "";
		try {
			result = client.execute("hello", "world");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(result);
		}
	}
}
