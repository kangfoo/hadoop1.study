/**
 * 
 */
package com.kangfoo.study.hadoop1.mp.sort;

import java.util.Random;

/**
 * 生成随机数
 * 
 * @date 2014年2月22日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public class RandomGenerator {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Random random = new Random();
		for (int i = 0; i < 100; i++) {
			int j = random.nextInt(2000);
			System.out.println(j);
		}
	}

}
