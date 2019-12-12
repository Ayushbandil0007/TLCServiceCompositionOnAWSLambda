package saaf;


/**
 * Created by Ayush Bandil on 12/12/2019.
 */

public class Response {
    //
    // User Defined Attributes
    //
    //
    // ADD getters and setters for custom attributes here.
    //

    // Return value
    private String value;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "value=" + this.getValue() + super.toString();
    }
}
