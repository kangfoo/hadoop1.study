/**
 * 
 */
package com.kangfoo.study.hadoop1.ipc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * @date 2013年12月26日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public interface MyRPCProtocol extends VersionedProtocol {

	Text test(Text t);

}
