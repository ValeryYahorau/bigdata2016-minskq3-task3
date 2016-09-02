package com.epam.bigdata2016.minskq3.task3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VisitSpendComparable implements WritableComparable<VisitSpendComparable> {

    private int visitsCount;
    private int spendsCount;

    public VisitSpendComparable() {
    }

    public VisitSpendComparable(int visitsCount, int spendsCount) {
        this.visitsCount = visitsCount;
        this.spendsCount = spendsCount;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(visitsCount);
        out.writeInt(spendsCount);
    }

    public void readFields(DataInput in) throws IOException {
        visitsCount = in.readInt();
        spendsCount = in.readInt();
    }

    public int compareTo(VisitSpendComparable w) {
        if (Integer.compare(visitsCount,w.getVisitsCount()) == 0 ) {
            return Integer.compare(spendsCount,w.getSpendsCount());
        } else {
            return Integer.compare(visitsCount,w.getVisitsCount());
        }
    }

    public int getVisitsCount() {
        return visitsCount;
    }

    public void setVisitsCount(int visitsCount) {
        this.visitsCount = visitsCount;
    }

    public int getSpendsCount() {
        return spendsCount;
    }

    public void setSpendsCount(int spendsCount) {
        this.spendsCount = spendsCount;
    }

    @Override
    public String toString() {
        return "Visits count : " + visitsCount + ", Bidding price sum : " + spendsCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VisitSpendComparable that = (VisitSpendComparable) o;

        if (getVisitsCount() != that.getVisitsCount()) return false;
        return getSpendsCount() == that.getSpendsCount();

    }

    @Override
    public int hashCode() {
        int result = getVisitsCount();
        result = 31 * result + getSpendsCount();
        return result;
    }
}