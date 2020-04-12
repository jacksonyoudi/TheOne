package com.bigdata.join;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Emplyee implements WritableComparable {
    private String empNo = "";
    private String name = "";
    private String deptNo = "";
    private String deptName = "";
    private int flag = 0;


    public Emplyee() {
    }

    public Emplyee(String empNo, String name, String deptNo, String deptName, int flag) {
        this.empNo = empNo;
        this.name = name;
        this.deptNo = deptNo;
        this.deptName = deptName;
        this.flag = flag;
    }


    public Emplyee(Emplyee e) {
        this.empNo = e.empNo;
        this.name = e.name;
        this.deptNo = e.deptNo;
        this.deptName = e.deptName;
        this.flag = e.flag;
    }


    public String getEmpNo() {
        return empNo;
    }

    public void setEmpNo(String empNo) {
        this.empNo = empNo;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDeptNo() {
        return deptNo;
    }

    public void setDeptNo(String deptNo) {
        this.deptNo = deptNo;
    }

    public String getDeptName() {
        return deptName;
    }

    public void setDeptName(String deptName) {
        this.deptName = deptName;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public int compareTo(Object o) {
        return 0;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.empNo);
        dataOutput.writeUTF(this.name);
        dataOutput.writeUTF(this.deptNo);
        dataOutput.writeUTF(this.deptName);
        dataOutput.writeInt(this.flag);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.empNo = dataInput.readUTF();
        this.name = dataInput.readUTF();
        this.deptNo = dataInput.readUTF();
        this.deptName = dataInput.readUTF();
        this.flag = dataInput.readInt();
    }

    @Override
    public String toString() {
        return empNo + ',' +
                name + ',' +
                deptNo + ',' +
                deptName + ',' +
                flag;
    }
}
