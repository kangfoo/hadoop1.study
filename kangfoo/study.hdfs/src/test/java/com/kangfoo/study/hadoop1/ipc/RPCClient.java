/**
 * 
 */
package com.kangfoo.study.hadoop1.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;

/**
 * @date 2013年12月26日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public class RPCClient {
	
	private MyRPCProtocol protocol;
	
	public RPCClient() throws IOException{
		InetSocketAddress addr = new InetSocketAddress("localhost",8888);
		protocol = (MyRPCProtocol)RPC.waitForProxy(MyRPCProtocol.class, 1, addr, new Configuration());
	}
	
	public void call(String s){
		System.out.println(protocol.test(new Text(s)));
	}

	public static void main(String[] args) throws IOException {
		RPCClient client = new RPCClient();
		client.call("RPCj");
	}
}
