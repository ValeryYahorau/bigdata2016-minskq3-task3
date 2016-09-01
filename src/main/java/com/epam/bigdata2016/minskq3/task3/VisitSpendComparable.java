package com.epam.bigdata2016.minskq3.task3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VisitSpendComparable implements WritableComparable<VisitSpendComparable> {

    private int visitsCount;
    private int spendsCount;

    public void write(DataOutput out) throws IOException {
        out.writeInt(visitsCount);
        out.writeInt(spendsCount);
    }

    public void readFields(DataInput in) throws IOException {
        visitsCount = in.readInt();
        spendsCount = in.readInt();
    }

    public int compareTo(VisitSpendComparable w) {
        if (visitsCount > w.visitsCount)
        {
            return 1;
        }
        else if (visitsCount < w.visitsCount)
        {
            return -1;
        }
        else {
            if (spendsCount > w.spendsCount)
            {
                return 1;
            }
            else if (spendsCount < w.spendsCount)
            {
                return -1;
            }
            else
            {
                return 0;
            }
        }

    }
}