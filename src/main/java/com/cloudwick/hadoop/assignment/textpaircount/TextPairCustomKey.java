package com.cloudwick.hadoop.assignment.textpaircount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPairCustomKey implements Writable, WritableComparable<TextPairCustomKey> {
    private Text state = new Text();
    private Text city = new Text();

    public Text getState() {
        return state;
    }

    public void setState(Text state) {
        this.state = state;
    }

    public Text getCity() {
        return city;
    }

    public void setCity(Text city) {
        this.city = city;
    }

    public void write(DataOutput dataOutput) throws IOException {
        state.write(dataOutput);
        city.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        state.readFields(dataInput);
        city.readFields(dataInput);
    }

    public static TextPairCustomKey read(DataInput in) throws Exception {
        TextPairCustomKey textPairCustomKey = new TextPairCustomKey();
        textPairCustomKey.readFields(in);
        return textPairCustomKey;
    }

    public boolean equals(Object object) {
        if (this == object)
            return true;

        if (object == null || getClass() != object.getClass())
            return false;

        TextPairCustomKey textPairCustomKey = (TextPairCustomKey) object;
        return state.equals(textPairCustomKey.state) && city.equals(textPairCustomKey.city);
    }

    public int compareTo(TextPairCustomKey o) {
        int comparedValue = state.compareTo(o.state);
        if(comparedValue == 0) {
            comparedValue = city.compareTo(o.city);
        }
        return comparedValue;
    }

    public int hashCode() {
        return state.hashCode() * 129 + city.hashCode();
    }

    public String toString() {
        return state + " : " + city;
    }
}
