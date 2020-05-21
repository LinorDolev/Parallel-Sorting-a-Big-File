package sortBigFile;

public class Record{
    private String[] values;

    public Record(String[] values) {
        setValues(values);
    }

    public String get(int index){
        if(index >= values.length)
            throw new IllegalArgumentException(
                    "Illegal index: " + index + ", max possible value: " + (values.length - 1));
        return values[index];
    }

    public String[] getValues() {
        return values;
    }

    private void setValues(String[] values) {
        this.values = values;
    }
}
