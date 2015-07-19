package storm.rpc;

import java.io.IOException;

public class MyBiz implements MyBizable {

	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return VERSION;
	}

	public String hello(String name) {
		System.out.println("我被调用了");
		return "hello "+name;
	}

}
