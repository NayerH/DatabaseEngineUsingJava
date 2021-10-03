import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable {
    String tableName;
    ArrayList<String> v;
    ArrayList<String> indices;
    
    public Table(String tableName){
          v = new ArrayList<String>();
          this.tableName = tableName;
          this.indices = new ArrayList<String>();
    }
}