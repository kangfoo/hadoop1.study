/**
 * 
 */
package com.kangfoo.study.hadoop1.mp.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @date 2014年2月21日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public class EmpDep implements WritableComparable {

	private String name = "";
	private String sex = "";
	private int age = 0;
	private int depNo = 0;
	private String depName = "";

	private String table = "";

	public EmpDep() {
	}

	public EmpDep(EmpDep empDep) {
		this.name = empDep.getName();
		this.sex = empDep.getSex();
		this.age = empDep.getAge();
		this.depNo = empDep.getDepNo();
		this.depName = empDep.getDepName();
	}

	@Override
	public String toString() {
		return name + " " + sex + " " + age + " " + depName;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public int getDepNo() {
		return depNo;
	}

	public void setDepNo(int depNo) {
		this.depNo = depNo;
	}

	public String getDepName() {
		return depName;
	}

	public void setDepName(String depName) {
		this.depName = depName;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeUTF(sex);
		out.writeInt(age);
		out.writeInt(depNo);
		out.writeUTF(depName);
		out.writeUTF(table);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.sex = in.readUTF();
		this.age = in.readInt();
		this.depNo = in.readInt();
		this.depName = in.readUTF();
		this.table = in.readUTF();
	}

	@Override
	public int compareTo(Object o) {
		return 0;
	}

}
