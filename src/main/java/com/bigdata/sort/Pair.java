package com.bigdata.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Pair implements WritableComparable<Pair> {
    private int first = 0;
    private int second = 0;


    public Pair(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public void set(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    public int compareTo(Pair o) {
        if (first != o.first) {
            return first - o.first;
        } else if (second != o.second) {
            return second - o.second;
        } else {
            return 0;
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(first);
        dataOutput.writeInt(second);
    }

    public void readFields(DataInput dataInput) throws IOException {
        first = dataInput.readInt();
        second = dataInput.readInt();
    }


    @Override
    public int hashCode() {
        return first + "".hashCode() + second + "".hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Pair) {
            Pair pair = (Pair) obj;
            return pair.first == this.first && pair.second == this.second;
        } else {
            return false;
        }
    }


}
