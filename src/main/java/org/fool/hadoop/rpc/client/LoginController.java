package org.fool.hadoop.rpc.client;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/*
 * 在Windows端运行，模拟RPC调用
 */
public class LoginController {
	public static void main(String[] args) throws IOException {
		LoginService proxy = RPC.getProxy(LoginService.class, 1L, new InetSocketAddress("hadoop-001", 10000), new Configuration());
	
		String result = proxy.login("hadoop", "RPC");
		
		System.out.println(result);
	}
}
