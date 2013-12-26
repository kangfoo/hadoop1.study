/**
 * 
 */
package com.kangfoo.study.hadoop1.ipc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

/**
 * @date 2013年12月26日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public class RPCServer implements MyRPCProtocol{

	Server server = null;
	
	public RPCServer() throws IOException, InterruptedException{
		server = RPC.getServer(this, "localhost", 8888, new Configuration());
		server.start();
		server.join();
	}
	
	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		new RPCServer();
	}

	@Override
	public long getProtocolVersion(String protocol, long clientVersion)
			throws IOException {
		return 1;
	}

	@Override
	public Text test(Text t) {
		if(t.toString().equals("RPC")){
			return new Text("1");
		}
		
		return new Text("0");
	}

}
