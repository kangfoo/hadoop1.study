package com.kangfoo.study.hadoop1.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.junit.BeforeClass;
import org.junit.Test;

public class WritableComparableTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Test
	public void test() throws IOException {
		String path ="/Users/kangfoo-mac/tmp.txt";
		
		Student s = new Student("kang",22,"man");
		FileOutputStream fopt = new FileOutputStream(new File(path)); 
		DataOutputStream out = new DataOutputStream(fopt);
		s.write(out);
		fopt.close();
		out.close();
		
		Student s1 = new Student();
		FileInputStream fin = new FileInputStream(new File(path));
		DataInputStream in = new DataInputStream(fin);
		
		s1.readFields(in);
		System.out.println(s1.toString());
	}
	
	
	private class Student implements WritableComparable{

		private Text name ;
		private IntWritable age ;
		private Text sex;
		
		public Student(){
			this.name = new Text();
			this.age = new IntWritable();
			this.sex = new Text();
		}
		
		public Student(String name, int age, String sex) {
			super();
			this.name = new Text(name);
			this.age = new IntWritable(age);
			this.sex = new Text(sex);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			name.write(out);
			age.write(out);
			sex.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			name.readFields(in); //name = in.readUTF();
			age.readFields(in);
			sex.readFields(in);
		}

		@Override
		public int compareTo(Object o) {
			int result = 0;
			Student s = (Student)o;
			if(0!=(result = name.compareTo(s.getName()))){
				return result;
			}
			if(0!=(result = age.compareTo(s.getAge()))){
				return result;
			}
			if(0!=(result = sex.compareTo(s.getSex()))){
				return result;
			}
			
			return result;
		}

		public Text getName() {
			return name;
		}

		public void setName(Text name) {
			this.name = name;
		}

		public IntWritable getAge() {
			return age;
		}

		public void setAge(IntWritable age) {
			this.age = age;
		}

		public Text getSex() {
			return sex;
		}

		public void setSex(Text sex) {
			this.sex = sex;
		}

		@Override
		public String toString() {
			return "Student [name=" + name + ", age=" + age + ", sex=" + sex
					+ "]";
		}
		
		
		
	}

}
