package lab2;

import java.io.Serializable;


/**
 * Simple class for a Log record. Needed for writing data to Cassandra
 * */

public class Log implements Serializable {
    private String key;
    private int value;

    public String getKey() {
        return this.key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getValue() {
        return this.value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
