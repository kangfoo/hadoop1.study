public class MyJNI{
	public native void print(String str);
	
	static {
		System.loadLibrary("MyJNI");
	}

	public static void main(String[] args){
		new MyJNI().print("jni callback!");
	}

}
