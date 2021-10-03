import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

public class Bucket implements Serializable {
    Vector<String> bucketVector;     //Index 0 stores pkValue and Index 1 stores page name

    public Bucket(){
        bucketVector = new Vector<String>();
    }
}
