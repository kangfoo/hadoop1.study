/**
 * 
 */
package com.kangfoo.study.hadoop1.io.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * @date 2013年12月26日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public class A implements CalculationProtocal {

	@Override
	public int add(int a, int b) {
		return a + b;
	}

	@Override
	public int sub(int a, int b) {
		return a - b;
	}
	
	public static void main(String[] args) {
		A a = new A();
		InvocationHandler handler = a.new CalculationHanlder(a);
		CalculationProtocal aProxy = (CalculationProtocal)Proxy.newProxyInstance(a.getClass().getClassLoader(), a.getClass().getInterfaces(), handler);
		System.out.println(aProxy.add(3, 5));
	}

	class CalculationHanlder implements InvocationHandler {

		private Object obj;

		public CalculationHanlder(Object obj) {
			this.obj = obj;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			System.out.println("before call invoke");
			Object rest = method.invoke(obj, args);
			System.out.println("after call invoke");
			return rest;
		}

	}
}
