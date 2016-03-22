package org.fool.hadoop.rpc.server;

import java.io.IOException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;
/*
 * 需要在VM中运行，模拟RPC调用
 */
public class RPCServer {
	public static void main(String[] args) throws HadoopIllegalArgumentException, IOException {
		Builder builder = new RPC.Builder(new Configuration());

		builder.setBindAddress("hadoop-001").setPort(10000).setProtocol(LoginService.class)
				.setInstance(new LoginServiceImpl());
		
		Server build = builder.build();
		
		build.start();
	}
}
