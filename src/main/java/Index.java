import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Vector;

public class Index implements Serializable {

    Hashtable<String,Vector<ArrayList<String>>> grid;
    ArrayList<String> colNames;
    Hashtable<String,ArrayList<String>> gridRange;

    public Index(String[] columnNames, Hashtable<String,ArrayList<String>> gridRange){
         //end in arraylist of strings
         this.colNames = new ArrayList<String> (Arrays.asList(columnNames));
         this.grid = new Hashtable<String,Vector<ArrayList<String>>>();
         for(int i = 0; i < colNames.size(); i++){
             Vector<ArrayList<String>> v = new Vector<ArrayList<String>>(10);
             for(int j = 0; j < 10; j++){
                 v.add(j,new ArrayList<String>());
             }
             this.grid.put(colNames.get(i),v);
         }
         this.gridRange = gridRange;
    }

}
