import java.io.Serializable;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements Serializable {
    String tableName;
    Object min;
    Object max;
    Vector<Hashtable<String,Object>> page;

    public Page(String tableName, String dataType){
        this.tableName = tableName;
        if(dataType.compareTo("java.lang.Integer") == 0)  {
            min = 0;
            max = 0;
        } else   if(dataType.compareTo("java.lang.String") == 0)  {
            min = new String();
            max = new String();
        }else if(dataType.compareTo("java.lang.Double") == 0)  {
            min = 0.0;
            max = 0.0;
        } else if(dataType.compareTo("java.util.Date") == 0)  {
            min = new Date();
            max = new Date();
        }
        page = new Vector<Hashtable<String, Object>>();
    }
}