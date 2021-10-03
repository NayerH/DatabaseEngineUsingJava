import java.io.*;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.time.*;

public class DBApp implements DBAppInterface{
    private int MaximumRowsCountinPage;
    private int MaximumKeysCountinIndexBucket;
    //QUESTIONS
    //WHAT IS MAVEN MAKE FILE
    //BINARY SEARCH USED WITHOUT INDEX OR ONLY FOR INDEX
    //GLOBAL INTs CORRECT?
    @Override
    public void init() {
        Properties prop = new Properties();
        String fileName = "src/main/resources/DBApp.config";
        InputStream is = null;
        try {
            is = new FileInputStream(fileName);
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
            return;
        }
        try {
            prop.load(is);
        } catch (IOException ex) {
            ex.printStackTrace();
            return;
        }
        MaximumRowsCountinPage = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
        MaximumKeysCountinIndexBucket = Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));

        File file = new File("src/main/resources/data");
        //Creating the directory
        file.mkdirs();
    }

    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        
        File csvFile = new File("src/main/resources/metadata.csv");
        if (csvFile.isFile()) {
            BufferedReader csvReader = null;
            try {
                csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
                String row;
                while ((row = csvReader.readLine()) != null) {
                    String[] data = row.split(",");
                    // do something with the data
                    if(data[0].compareTo(tableName) == 0)    {
                            throw new DBAppException("A table with this name already exists");
                    }
                }
                csvReader.close();
                Table t = new Table(tableName);
                Enumeration enu = colNameType.keys();
                PrintWriter pw = null;
                StringBuilder builder = null;
                try {
                    pw = new PrintWriter(new FileWriter("src/main/resources/metadata.csv",true));

                } catch (FileNotFoundException e) {
                    throw e;
                }
                while (enu.hasMoreElements()) {
                    builder = new StringBuilder();
                    String col = (String) enu.nextElement();
                    String colType = colNameType.get(col);
                    if(colType == null){
                        throw new DBAppException("Column included without its type");
                    } else {
                        if(colType.compareTo("java.lang.Integer") != 0 && colType.compareTo("java.lang.String") != 0 && colType.compareTo("java.lang.Double") != 0 && colType.compareTo("java.util.Date") != 0 ){
                            throw new DBAppException("Invalid Data Type");
                        }
                    }
                    String clusteringKeyStr = "False";
                    if(clusteringKey.compareTo(col) == 0){
                        clusteringKeyStr = "True";
                    }
                    String colMin = colNameMin.get(col);
                    String colMax = colNameMax.get(col);
                    if(colMin == null || colMax == null){
                        throw new DBAppException("Invalid Column Minimum/Maximum");
                    }

                    builder.append(tableName + "," + col + "," + colType + "," + clusteringKeyStr + ",False," + colMin +"," + colMax);
                    builder.append('\n');
                    pw.write(builder.toString());
                }
                pw.close();
                try {
                    String outputLocation = "src/main/resources/data/" + tableName + ".ser";
                    FileOutputStream fileOut =
                            new FileOutputStream(outputLocation);
                    ObjectOutputStream out = new ObjectOutputStream(fileOut);
                    out.writeObject(t);
                    out.close();
                    fileOut.close();
                } catch (IOException i) {
                    i.printStackTrace();
                    return;
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return;
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }

        }
    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {

        //Check colNames if valid                DONE
        //Check if there is an index before      DONE
        //Helper method to calculate the ranges   DONE
        //Save Index serialize with correct name    DONE
        //Fill buckets                               DONE
        //Save Table to save new index in the arraylist with correct name     DONE
        
        File csvFile = new File("src/main/resources/metadata.csv");
        Hashtable<String,String> colInfo = new Hashtable<String,String>();
        String pkDataType = "";
        String pkColName = "";
        if (csvFile.isFile()) {
            BufferedReader csvReader = null;
            try{
                csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
                String row;
                while ((row = csvReader.readLine()) != null) {
                    String[] data = row.split(",");
                    // do something with the data
                    if(data[0].compareTo(tableName) == 0)    {
                        if(data[3].compareTo("True") == 0)     {
                            pkDataType = data[2];
                            pkColName = data[1];
                        }
                        String s = data[2]+"," + data[5]+"," + data[6];    //type min max
                        colInfo.put(data[1], s);
                    }
                }

                csvReader.close();
            } catch (FileNotFoundException e) {
                throw new DBAppException("MetaData CSV not found");
            } catch (IOException e) {
                throw new DBAppException("MetaData CSV not found");
            }

            for(int i = 0; i < columnNames.length; i++) {
                if(!(colInfo.containsKey(columnNames[i]))) {
                    throw new DBAppException("Invalid Column name input");
                }
            }
            
            Table t = loadTable(tableName);
            if(t==null){
                throw new DBAppException("Table not found!");
            }
            for(int j = 0; j < t.indices.size(); j++) {
                ArrayList<String> indexCols = new ArrayList<String> (Arrays.asList(t.indices.get(j).split(",")));
                //Remove Table Name and .ser at the end
                indexCols.remove(0);
                indexCols.remove(indexCols.size() - 1);
                boolean valid = false;
                for(int columnNamesI = 0; columnNamesI < columnNames.length; columnNamesI++){
                  if(!indexCols.contains(columnNames[columnNamesI])){
                        valid = true;
                  }
                }
                if(!valid && (indexCols.size() == columnNames.length)){
                    throw new DBAppException("Index already created");
                }
            }

            String indexName = tableName + ",";
            for(String columnName : columnNames){
                indexName += columnName  + ",";
            }
            indexName += ".ser";

            Hashtable<String,ArrayList<String>> gridR = this.getRange(colInfo,columnNames);
            Index i = new Index(columnNames, gridR);

            for(int pIndex = 0; pIndex < t.v.size(); pIndex++){
                String pageName = t.v.get(pIndex);
                String pageLocation = "src/main/resources/data/" + pageName;
                Page p = loadPage(pageName);
                for(int rowN = 0; rowN < p.page.size(); rowN++){
                    Hashtable<String,Object> row = p.page.get(rowN);
                    ArrayList<Integer> indexLocation = new ArrayList<Integer>();
                    for(String colName: i.colNames){
                        String dataType = colInfo.get(colName).split(",")[0];
                        indexLocation.add(this.binSearch(dataType, gridR.get(colName), row.get(colName)));
                    }
                    ArrayList<String> buckets = new ArrayList<String>(i.grid.get(i.colNames.get(0)).get(indexLocation.get(0)));
                    for(int j = 1; j < i.colNames.size(); j++){
                        buckets.retainAll(i.grid.get(i.colNames.get(j)).get(indexLocation.get(j)));
                    }
                    //MIGHT CHANGE BUCKET ROW
                    String bucketRow = row.get(pkColName) + "," + pageName;
                    if(buckets.size() == 0){
                        Bucket newB = new Bucket();
                        newB.bucketVector.add(bucketRow);
                        String bucketName = indexName.substring(0,indexName.length() - 4);
                        for(int z = 0; z < indexLocation.size(); z++){
                            bucketName+= indexLocation.get(z) + ",";
                        }
                        bucketName += "1,.ser";
                        int iLoc = 0;
                        for(String colName: columnNames){
                            i.grid.get(colName).get(indexLocation.get(iLoc)).add(bucketName);
                            iLoc++;
                        }
                        saveBucket(newB, bucketName);
                    } else {
                       String bucketName = Collections.max(buckets);
                       Bucket b = loadBucket(bucketName);
                       if(b.bucketVector.size() == this.MaximumKeysCountinIndexBucket){
                           Bucket newB = new Bucket();
                           newB.bucketVector.add(bucketRow);
                           String[] newBName = bucketName.split(",");
                           bucketName = "";
                           for(int ind = 0; ind < (newBName.length - 2); ind++){
                               bucketName += newBName[ind] + ",";
                           }
                           bucketName += (buckets.size() + 1) + ",.ser";
                           int iLoc = 0;
                           for(String colName: columnNames){
                               i.grid.get(colName).get(indexLocation.get(iLoc)).add(bucketName);
                               iLoc++;
                           }
                           saveBucket(newB, bucketName);
                       } else {
                           b.bucketVector.add(bucketRow);
                           saveBucket(b, bucketName);
                       }
                    }
                }
                p = null;
            }

            try {
                saveIndex(i,indexName);
            } catch (IOException e) {
                System.out.println("Error saving Index");
                e.printStackTrace();
                return;
            }
            String[] checkPrimaryKey = indexName.split(",");
            if(checkPrimaryKey[1].equals(pkColName) && (checkPrimaryKey.length == 3)) {
                t.indices.add(0,indexName);
            } else {
                t.indices.add(indexName);
            }

            saveTable(t);

        } else {
            throw new DBAppException("MetaData CSV not found");
        }

    }

    public Bucket loadBucket(String bucketName) {
        Bucket b = null;
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + bucketName);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            b = (Bucket) in.readObject();
            in.close();
            fileIn.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("Error deserializing bucket");
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error deserializing bucket");
            return null;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("Error deserializing bucket");
            return null;
        }
        return b;
    }

    public void saveBucket(Bucket b, String bucketName) {
        String pageLocation = "src/main/resources/data/" + bucketName;
        FileOutputStream fileOut =
                null;
        try {
            fileOut = new FileOutputStream(pageLocation);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(b);
            out.close();
            fileOut.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("Error saving bucket");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error saving bucket");

        }
    }

    public Index loadIndex(String indexName) {
        Index i = null;
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + indexName);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            i = (Index) in.readObject();
            in.close();
            fileIn.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("Error deserializing index");
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error deserializing index");
            return null;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("Error deserializing index");
            return null;
        }
        return i;
    }

    public Integer binSearch(String dataType, ArrayList<String> range, Object o) {
        if(o == null){
            return 0;
        }
        int first  = 0;
        int last = 9;
        while(first <= last){
            int mid = (first + last)/2;
            String[] minMax = range.get(mid).split(",");
            if(dataType.equals("java.util.Date")){
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                try {
                    Date dateMin = formatter.parse(minMax[0]);
                    Date dateMax = formatter.parse(minMax[1]);
                    if((((Date)o).compareTo(dateMin) >= 0) && ((Date)o).compareTo(dateMax) <= 0){
                       return mid;
                    } else if((((Date)o).compareTo(dateMin) < 0)){
                       last = mid - 1;
                    } else {
                       first = mid + 1;
                    }
                } catch (ParseException e) {
                    System.out.println("Cannot parse date");
                    e.printStackTrace();
                }
            } else if(dataType.equals("java.lang.String")){
                 Integer strMin = Integer.parseInt(minMax[0]);
                 Integer strMax = Integer.parseInt(minMax[1]);
                 Integer asciiVal = this.calculateAscii((String)o);
                    if(((asciiVal).compareTo(strMin) >= 0) && (asciiVal).compareTo(strMax) <= 0){
                        return mid;
                    } else if(((asciiVal).compareTo(strMin) < 0)){
                        last = mid - 1;
                    } else {
                        first = mid + 1;
                    }
            } else if(dataType.equals("java.lang.Integer")){
                Integer intMin = Integer.parseInt(minMax[0]);
                Integer intMax = Integer.parseInt(minMax[1]);
                if((((Integer)o).compareTo(intMin) >= 0) && (((Integer)o).compareTo(intMax) <= 0)){
                    return mid;
                } else if((((Integer)o).compareTo(intMin) < 0)){
                    last = mid - 1;
                } else {
                    first = mid + 1;
                }
            } else{
                Double dobMin = Double.parseDouble(minMax[0]);
                Double dobMax = Double.parseDouble(minMax[1]);
                if((((Double)o).compareTo(dobMin) >= 0) && (((Double)o).compareTo(dobMax) <= 0)){
                    return mid;
                } else if((((Double)o).compareTo(dobMin) < 0)){
                    last = mid - 1;
                } else {
                    first = mid + 1;
                }
            }
        }
        System.out.println("Cannot find range in binary search");
        System.out.println(o);
        return null;
    }

    public void saveIndex(Index i, String indexName) throws IOException {

        String pageLocation = "src/main/resources/data/" + indexName;
        FileOutputStream fileOut =
                null;
        try {
            fileOut = new FileOutputStream(pageLocation);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(i);
            out.close();
            fileOut.close();
        } catch (FileNotFoundException e) {
            throw e;
        } catch (IOException e) {
            throw e;
        }
    }

    
    public Hashtable<String,ArrayList<String>> getRange (Hashtable<String,String> colInfo, String[] colNames){
        Hashtable<String,ArrayList<String>> result = new Hashtable<String,ArrayList<String>>();
        ArrayList<String> colNamesList = new ArrayList<String>(Arrays.asList(colNames));
        Enumeration<String> en = colInfo.keys();
        while(en.hasMoreElements()){
            String colNameStr = en.nextElement();
            if(colNamesList.contains(colNameStr)) {
                String values = colInfo.get(colNameStr);
                String[] valuesSplit = values.split(",");
                switch (valuesSplit[0]){
                    case "java.util.Date":
                        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                        try {
                            Date dateMin = formatter.parse(valuesSplit[1]);
                            Date dateMax = formatter.parse(valuesSplit[2]);
                            long daysBetween = Duration.between(dateMin.toInstant(), dateMax.toInstant()).toDays();
                            long interval = daysBetween/10;
//                            System.out.println(interval);
                            ArrayList<String> res = new ArrayList<String>();

                            for(int i = 0; i < 10 ; i++){
                                String s = formatter.format(dateMin) + ",";
                                Calendar c = Calendar.getInstance();
                                c.setTime(dateMin);
                                c.add(Calendar.DAY_OF_MONTH, (int) interval);
                                String dateMaxStr = null;
                                if(i != 9){
                                    dateMaxStr = formatter.format(c.getTime());
                                } else {
                                    dateMaxStr = formatter.format(dateMax);
                                }
                                s += dateMaxStr;
                                res.add(i,s);
                                dateMin = formatter.parse(dateMaxStr);
                            }
                            result.put(colNameStr, res);
                            
                        } catch (ParseException e) {
                            e.printStackTrace();
                            System.out.println("Error in parsing date in getting ranges");
                            return null;
                        }
                        break;
                    case "java.lang.Integer":
                        Integer intMin = Integer.parseInt(valuesSplit[1]);
                        Integer intMax = Integer.parseInt(valuesSplit[2]);
                        Integer rangeBetween = intMax - intMin;
                        Integer interval = rangeBetween/10;
                        ArrayList<String> res = new ArrayList<String>();
                        for(int i = 0; i < 10 ; i++){
                            String s = intMin + ",";
                            intMin += interval;
                            if(i == 9){
                               s += intMax;
                            } else {
                               s+= intMin;
                            }
                            res.add(i,s);
                        }
                        result.put(colNameStr, res);
                        break;
                    case "java.lang.Double":
                        Double dobMin = Double.parseDouble(valuesSplit[1]);
                        Double dobMax = Double.parseDouble(valuesSplit[2]);
                        Double rangeBetweenDouble = dobMax - dobMin;
                        Double intervalDouble = rangeBetweenDouble/10;
                        ArrayList<String> resDob = new ArrayList<String>();
                        for(int i = 0; i < 10 ; i++){
                            String s = dobMin + ",";
                            dobMin += intervalDouble;
                            if(i == 9){
                                s += dobMax;
                            } else {
                                s+= dobMin;
                            }
                            resDob.add(i,s);
                        }
                        result.put(colNameStr, resDob);
                        break;
                    case "java.lang.String":
                        String strMin = valuesSplit[1];
                        String strMax = valuesSplit[2];

                        int asciiMax = this.calculateAscii(strMax);
                        int asciiMin = this.calculateAscii(strMin);
                        int intervalAscii = (asciiMax - asciiMin) /10;
                        ArrayList<String> res2 = new ArrayList<String>();
                        for(int i = 0; i < 10 ; i++){
                            String s = asciiMin + ",";
                            asciiMin += intervalAscii;
                            if(i == 9){
                                s += asciiMax;
                            } else {
                                s+= asciiMin;
                            }
                            res2.add(i,s);
                        }
                        result.put(colNameStr, res2);
                        break;
                    default: break;
                }

            }

        }
        return result;
    }


    public Table loadTable(String tableName){
        Table t = null;
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + tableName + ".ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            t = (Table) in.readObject();
            in.close();
            fileIn.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("Error deserializing table");
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error deserializing table");
            return null;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("Error deserializing table");
            return null;
        }
        return t;
    }

    public Page loadPage(String pageLocation){
        Page p = null;
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + pageLocation);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            p = (Page) in.readObject();
            in.close();
            fileIn.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("Error deserializing page");
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error deserializing page");
            return null;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("Error deserializing page");
            return null;
        }
        return p;
    }

    public int calculateAscii(String s){
        int l = s.length();
        int res = 0;
        for(int i = 0; i < l; i++){
            int ascii = (int) s.charAt(i);
            res = res * 10;
            res += ascii;
        }
        return res;
    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {

        //Cases:
        //no clustering key entered   DONE
        //verify input data           DONE
        // 2: Initial Page            DONE
 

        //MAY NEED TO UPDATE MIN AND MAX

        //WITHIN RANGE: EITHER WAYS INSERT - THEN CHECK IF MORE THAN MAX, OVERFLOW  DONE
        //MORE THAN MAX 1st PAGE AND SMALLER THAN 2ND PAGE: INSERT IN THE EMPTY PAGE and update max and min
        //                                                  NO EMPTY: OVERFLOW

        Table t = null;
        try {
//            System.out.println(tableName);
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + tableName + ".ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            t = (Table) in.readObject();
            in.close();
            fileIn.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        } catch (IOException e) {
            e.printStackTrace();
            return;

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;

        }
        String pkDataType = "";
        String pkColName = "";
        Hashtable<String,String> colInfo = new Hashtable<String,String>();
        File csvFile = new File("src/main/resources/metadata.csv");
        if (csvFile.isFile()) {
            BufferedReader csvReader = null;
            try {
                Hashtable<String,Object> oldRowToBeUpdatedInIndex = new Hashtable<String,Object>();
                String oldRowToBeUpdatedInIndexBucketOld = "";
                String oldRowToBeUpdatedInIndexBucketNew = "";
                csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
                String row;
                while ((row = csvReader.readLine()) != null) {
                    String[] data = row.split(",");
                    // do something with the data
                    if(data[0].compareTo(tableName) == 0)    {
                        if(data[3].compareTo("True") == 0)     {
                            pkDataType = data[2];
                            pkColName = data[1];
                        }
                        String s = data[2]+"," + data[5]+"," + data[6];    //type min max
                        colInfo.put(data[1], s);

                    }
                }

                csvReader.close();
                if(!colNameValue.containsKey(pkColName)) {
                    throw new DBAppException("No clustering key entered");
                }
                Enumeration<String> en = colNameValue.keys();
                while(en.hasMoreElements()){
                    String colNameStr = en.nextElement();
                    Object valueToBeInserted = colNameValue.get(colNameStr);
                    if(colInfo.containsKey(colNameStr))   {
                        String[] colInfoExtract =  colInfo.get(colNameStr).split(",");
                        switch (colInfoExtract[0]){
                            case "java.util.Date":
                                if(valueToBeInserted instanceof Date){
                                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                                    try {
                                        Date dateMin = formatter.parse(colInfoExtract[1]);
                                        Date dateMax = formatter.parse(colInfoExtract[2]);
                                        if(((Date)valueToBeInserted).compareTo(dateMin) < 0 || ((Date)valueToBeInserted).compareTo(dateMax) > 0) {
//                                            System.out.println(valueToBeInserted);
                                            throw new DBAppException("Data out of bounds specified in Metadata file");
                                        }
                                    } catch (ParseException e) {
                                        e.printStackTrace();
                                        return;
                                    }

                                }   else{
                                    throw new DBAppException("Invalid data type input");
                                }
                                break;
                            case "java.lang.Integer":
                                if(valueToBeInserted instanceof Integer){
                                    Integer intMin = Integer.parseInt(colInfoExtract[1]);
                                    Integer intMax = Integer.parseInt(colInfoExtract[2]);
                                    if(((Integer)valueToBeInserted).compareTo(intMin) < 0 || ((Integer)valueToBeInserted).compareTo(intMax) > 0) {
                                        throw new DBAppException("Data out of bounds specified in Metadata file");
                                    }
                                }   else{
                                    throw new DBAppException("Invalid data type input");
                                }
                                break;
                            case "java.lang.Double":
                                if(valueToBeInserted instanceof Double){
                                    Double dobMin = Double.parseDouble(colInfoExtract[1]);
                                    Double dobMax = Double.parseDouble(colInfoExtract[2]);
                                    if(((Double)valueToBeInserted).compareTo(dobMin) < 0 || ((Double)valueToBeInserted).compareTo(dobMax) > 0) {
                                        throw new DBAppException("Data out of bounds specified in Metadata file");
                                    }
                                }   else{
                                    throw new DBAppException("Invalid data type input");
                                }
                                break;
                            case "java.lang.String":
                                if(valueToBeInserted instanceof String){
                                    String strMin =   colInfoExtract[1];
                                    String strMax =   colInfoExtract[2];
                                    if(((String)valueToBeInserted).compareTo(strMin) < 0 || ((String)valueToBeInserted).compareTo(strMax) > 0) {
                                        throw new DBAppException("Data out of bounds specified in Metadata file");
                                    }
                                }  else{
                                    throw new DBAppException("Invalid data type input");
                                }
                                break;
                            default: break;
                        }
                    } else {
                        throw new DBAppException("Column input not found");
                    }
                }
                //CHECK IF THERE IS AN INDEX ON PRIMARY KEY
                boolean indexFound = false;
                String indexName = "";
                for(int indices = 0; indices < t.indices.size(); indices++){
                    ArrayList<String> indexSplit = new ArrayList<String> (Arrays.asList(t.indices.get(indices).split(",")));
                    if(indexSplit.contains(pkColName)){
                        indexFound = true;
                        indexName =  t.indices.get(indices);
                        break;
                    }
                }
                boolean usedBucket = false;
                String pageNameIndex = null;
                if(indexFound){
                    Index i = loadIndex(indexName);
                    ArrayList<Integer> indexLocation = new ArrayList<Integer>();
                    for(String colName: i.colNames){
                        String dataType = colInfo.get(colName).split(",")[0];
                        indexLocation.add(this.binSearch(dataType, i.gridRange.get(colName), colNameValue.get(colName)));
                    }
                    ArrayList<String> buckets = new ArrayList<String>(i.grid.get(i.colNames.get(0)).get(indexLocation.get(0)));
                    for(int j = 1; j < i.colNames.size(); j++){
                        buckets.retainAll(i.grid.get(i.colNames.get(j)).get(indexLocation.get(j)));
                    }
                    if(buckets.size() != 0){

                        String finalpkDataType = pkDataType;
                        Comparator<String> c1 = new Comparator<String>() {
                            public int compare(String u1, String u2)
                            {
                                String u1Object = u1.split(",")[0];
                                String u2Object = u2.split(",")[0];
                                if(finalpkDataType.equals("java.util.Date")){
                                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                                    try {
                                        Date u1Date = formatter.parse(u1Object);
                                        Date u2Date = formatter.parse(u2Object);
                                        return compareTo(u1Date,u2Date);
                                    } catch (ParseException e) {
                                        e.printStackTrace();
                                    }
                                } else if(finalpkDataType.equals("java.lang.Integer")){
                                        Integer u1Int = Integer.parseInt(u1Object);
                                        Integer u2Int = Integer.parseInt(u2Object);
                                        return compareTo(u1Int, u2Int);
                                } else if(finalpkDataType.equals("java.lang.Double")){
                                        Double u1Int = Double.parseDouble(u1Object);
                                        Double u2Int = Double.parseDouble(u2Object);
                                        return compareTo(u1Int, u2Int);
                                }
                                return compareTo(u1Object,u2Object);
                            }
                        };
                        for(int buckI = 0; buckI < buckets.size(); buckI++){
                            Bucket b = loadBucket(buckets.get(buckI));
                            String pkValue = colNameValue.get(pkColName) + "";
                            if(pkDataType.equals("java.util.Date")){
                                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                                pkValue = formatter.format(colNameValue.get(pkColName));
                            }
                            int indexInBucket = Collections.binarySearch(b.bucketVector,pkValue, c1);
                            if(indexInBucket >= 0){
                                throw new DBAppException("Primary Key already found, cannot insert duplicates.");
                            }
                            indexInBucket = Math.abs(indexInBucket + 1);
                            pageNameIndex = null;
                            if(indexInBucket == b.bucketVector.size()){
                                //EDIT IF STRING ROW OF BUCKET CHANGES
                                if(buckI == (buckets.size() -1)){
                                    String test =  b.bucketVector.get(indexInBucket-1);
//                                    System.out.println(test);
                                   pageNameIndex = test.split(",")[1];
                                   usedBucket = true;
                                } else {
                                    continue;
                                }
                            } else {
                                pageNameIndex = b.bucketVector.get(indexInBucket).split(",")[1];
                                usedBucket = true;
                            }
                            if(pageNameIndex != null){
                              break;
                            }
                        }
                    }
                }
                String targetPage = "";
                if(t.v.size() == 0){
                    //NO PAGES, CREATE FIRST PAGE
                    Page newPage = new Page(tableName, pkDataType);
                    newPage.page.add(colNameValue);
                    newPage.min = newPage.page.get(0).get(pkColName);
                    newPage.max = newPage.page.get(newPage.page.size() - 1).get(pkColName);
                    targetPage = t.tableName + t.v.size() + ".ser";
                    String pageLocation = "src/main/resources/data/" + targetPage;
                    String tablePageName = t.tableName + t.v.size() + ".ser";
                    t.v.add(tablePageName);
                    savePage(newPage, pageLocation);
                    saveTable(t);

                } else {
                   boolean stop = false;
                   int indexOfIndexPage = t.v.indexOf(pageNameIndex);
                   for(int pindex = 0; pindex < t.v.size(); pindex++){
                       if(stop){
                           break;
                       }
                       if(usedBucket){
                          if(pindex < indexOfIndexPage){
                              continue;
                          }
                       }
                       Page p;
                       targetPage = t.v.get(pindex);
                       String pageLocation = "src/main/resources/data/" + t.v.get(pindex);
                       try {
                           FileInputStream fileIn = new FileInputStream(pageLocation);
                           ObjectInputStream in = new ObjectInputStream(fileIn);
                           p = (Page) in.readObject();
                           in.close();
                           fileIn.close();
                       } catch (Exception e){
                           System.out.println(colNameValue.get(pkColName));
                           throw new DBAppException("Page not found!");
                       }
                       String finalColName = pkColName;
                       Comparator<Hashtable<String,Object>> c = new Comparator<Hashtable<String,Object>>() {
                           public int compare(Hashtable<String,Object> u1, Hashtable<String,Object> u2)
                           {
                               Object u1Object = u1.get(finalColName);
                               Object u2Object = u2.get(finalColName);
                               if(u1Object instanceof Date){
                                   return ((Date)u1Object).compareTo((Date)u2Object);
                               } else if(u1Object instanceof String){
                                   return ((String)u1Object).compareTo((String)u2Object);
                               } else if(u1Object instanceof Integer){
                                   return ((Integer)u1Object).compareTo((Integer)u2Object);
                               } else {
                                   return ((Double)u1Object).compareTo((Double)u2Object);
                               }
                           }
                       };
                       int index = Collections.binarySearch(p.page,colNameValue,c);
                       if(index >= 0){
                           throw new DBAppException("Primary Key already found, cannot insert duplicates.");
                       }
                       if((compareTo(colNameValue.get(pkColName), p.min) >= 0) && (compareTo(colNameValue.get(pkColName), p.max) <= 0)){
                           p.page.add(colNameValue);

                           p.page.sort(c);
                           if(p.page.size() > this.MaximumRowsCountinPage){
                               Hashtable<String, Object> lastRow = p.page.remove(p.page.size() - 1);
                               oldRowToBeUpdatedInIndex = lastRow;
                               oldRowToBeUpdatedInIndexBucketOld = lastRow.get(pkColName) + "," + t.v.get(pindex);
                               try{
                                   String nextPageName = t.v.get(pindex + 1);

                                   Page nextP;
                                   String pageNextLocation = "src/main/resources/data/" + nextPageName;
                                   try {
                                       FileInputStream fileIn = new FileInputStream(pageNextLocation);
                                       ObjectInputStream in = new ObjectInputStream(fileIn);
                                       nextP = (Page) in.readObject();
                                       in.close();
                                       fileIn.close();
                                   } catch (Exception e){
                                       throw new DBAppException("Page not found!");
                                   }
                                   if(nextP.page.size() == this.MaximumRowsCountinPage){
                                       throw new IndexOutOfBoundsException();
                                   } else {
                                       nextP.page.add(lastRow);
                                       nextP.page.sort(c);
                                       nextP.max = nextP.page.get(nextP.page.size() - 1).get(pkColName);
                                       nextP.min = nextP.page.get(0).get(pkColName);
                                       oldRowToBeUpdatedInIndexBucketNew = lastRow.get(pkColName) + "," +nextPageName;
                                       savePage(nextP, pageNextLocation);
                                   }
                               } catch (IndexOutOfBoundsException e){
                                   Page newPageOverflow = new Page(tableName, pkDataType);
                                   newPageOverflow.page.add(lastRow);
                                   newPageOverflow.min = newPageOverflow.page.get(0).get(pkColName);
                                   newPageOverflow.max = newPageOverflow.page.get(newPageOverflow.page.size() - 1).get(pkColName);
                                   String pageLocationOverflow = "src/main/resources/data/" + t.tableName + t.v.size() + ".ser";
                                   oldRowToBeUpdatedInIndexBucketNew = lastRow.get(pkColName) + "," + (t.tableName + t.v.size() + ".ser");
                                   t.v.add(pindex+1,t.tableName + t.v.size() + ".ser");
                                   savePage(newPageOverflow, pageLocationOverflow);
                               }
                           }
                           p.min = p.page.get(0).get(pkColName);
                           p.max = p.page.get(p.page.size() - 1).get(pkColName);
                           savePage(p, pageLocation);
                           saveTable(t);
                           stop = true;
                       } else if(compareTo(colNameValue.get(pkColName), p.min) < 0){
                           if(p.page.size() == this.MaximumRowsCountinPage){
                               try{
                                   String prevPageName = t.v.get(pindex - 1);
                                   Page prevP;

                                   String pagePrevLocation = "src/main/resources/data/" + prevPageName;
                                   try {
                                       FileInputStream fileIn = new FileInputStream(pagePrevLocation);
                                       ObjectInputStream in = new ObjectInputStream(fileIn);
                                       prevP = (Page) in.readObject();
                                       in.close();
                                       fileIn.close();
                                   } catch (Exception e){
                                       throw new DBAppException("Page not found!");
                                   }
                                   if(prevP.page.size() == this.MaximumRowsCountinPage){
                                       throw new IndexOutOfBoundsException();
                                   }
                                   prevP.page.add(colNameValue);
                                   targetPage = prevPageName;
                                   prevP.page.sort(c);
                                   prevP.min = prevP.page.get(0).get(pkColName);
                                   prevP.max = prevP.page.get(prevP.page.size() - 1).get(pkColName);
                                   savePage(prevP, pagePrevLocation);
                                   saveTable(t);
                               } catch (IndexOutOfBoundsException e) {
                                   Page newPageOverflow = new Page(tableName, pkDataType);
                                   newPageOverflow.page.add(colNameValue);
                                   newPageOverflow.min = newPageOverflow.page.get(0).get(pkColName);
                                   newPageOverflow.max = newPageOverflow.page.get(newPageOverflow.page.size() - 1).get(pkColName);
                                   String pageLocationOverflow = "src/main/resources/data/" + t.tableName + t.v.size() + ".ser";
                                   targetPage = t.tableName + t.v.size() + ".ser";
                                   t.v.add(pindex,t.tableName + t.v.size() + ".ser");
                                   savePage(newPageOverflow, pageLocationOverflow);
                                   saveTable(t);
                               }
                           } else {
                               p.page.add(colNameValue);
                               p.page.sort(c);
                               p.min = p.page.get(0).get(pkColName);
                               p.max = p.page.get(p.page.size() - 1).get(pkColName);
                               savePage(p, pageLocation);
                               saveTable(t);
                           }
                           stop = true;
                       } else {
                           //CHECK IF LAST PAGE
                           if(pindex == (t.v.size()-1)) {
                               if(p.page.size() == this.MaximumRowsCountinPage){
                                   Page newPageOverflow = new Page(tableName, pkDataType);
                                   newPageOverflow.page.add(colNameValue);
                                   newPageOverflow.min = newPageOverflow.page.get(0).get(pkColName);
                                   newPageOverflow.max = newPageOverflow.page.get(newPageOverflow.page.size() - 1).get(pkColName);
                                   String pageLocationOverflow = "src/main/resources/data/" + t.tableName + t.v.size() + ".ser";
                                   targetPage = t.tableName + t.v.size() + ".ser";
                                   t.v.add(pindex+1,t.tableName + t.v.size() + ".ser");
                                   savePage(newPageOverflow, pageLocationOverflow);
                                   saveTable(t);
                               } else {
                                   p.page.add(colNameValue);
                                   p.page.sort(c);
                                   p.min = p.page.get(0).get(pkColName);
                                   p.max = p.page.get(p.page.size() - 1).get(pkColName);
                                   savePage(p, pageLocation);
                                   saveTable(t);
                               }
                               stop = true;
                           }
                       }
                   }
                }
                String pkValue = colNameValue.get(pkColName) + "";
                if(pkDataType.equals("java.util.Date")){
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                    pkValue = formatter.format(colNameValue.get(pkColName));
                }
                //EDIT BUCKET ROW IF NEEDED
                String bucketRow = pkValue + "," + targetPage;
                for(int tableIndex = 0; tableIndex < t.indices.size(); tableIndex++){
                    Index i = loadIndex(t.indices.get(tableIndex));
                    i = insertIntoIndex(i, t.indices.get(tableIndex), colInfo, colNameValue, bucketRow,pkValue,pkDataType);
                    if(!oldRowToBeUpdatedInIndexBucketOld.equals("")){
                        String pkValueOld = oldRowToBeUpdatedInIndexBucketOld.split(",")[0];
                        String pkValueNew = oldRowToBeUpdatedInIndexBucketNew.split(",")[0];

                        if(pkDataType.equals("java.util.Date")){
                            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                            pkValueOld = formatter.format(oldRowToBeUpdatedInIndex.get(pkColName));
                            pkValueNew = formatter.format(oldRowToBeUpdatedInIndex.get(pkColName));
                            oldRowToBeUpdatedInIndexBucketNew = pkValueNew + "," +oldRowToBeUpdatedInIndexBucketNew.split(",")[1];
                            oldRowToBeUpdatedInIndexBucketOld = pkValueOld + "," +oldRowToBeUpdatedInIndexBucketOld.split(",")[1];
                        }
                        i = deleteFromIndex(i, t.indices.get(tableIndex), colInfo, oldRowToBeUpdatedInIndex, oldRowToBeUpdatedInIndexBucketOld,pkValueOld,pkDataType);
                        i = insertIntoIndex(i, t.indices.get(tableIndex), colInfo, oldRowToBeUpdatedInIndex, oldRowToBeUpdatedInIndexBucketNew,pkValueNew,pkDataType);
                    }
                    saveIndex(i, t.indices.get(tableIndex));
                }
            } catch (FileNotFoundException e) {
                throw new DBAppException("MetaData CSV not found");
            } catch (IOException e) {
                throw new DBAppException("MetaData CSV not found");
            }

        }  else {
            throw new DBAppException("MetaData CSV not found");
        }

    }

    public Index insertIntoIndex(Index i, String indexName, Hashtable<String,String> colInfo, Hashtable<String,Object> colNameValue, String bucketRow, String pkValue, String pkDataType){

        ArrayList<Integer> indexLocation = new ArrayList<Integer>();
        for(String colName: i.colNames){
            String dataType = colInfo.get(colName).split(",")[0];
            indexLocation.add(this.binSearch(dataType, i.gridRange.get(colName), colNameValue.get(colName)));
        }
        ArrayList<String> buckets = new ArrayList<String>(i.grid.get(i.colNames.get(0)).get(indexLocation.get(0)));
        for(int j = 1; j < i.colNames.size(); j++){
            buckets.retainAll(i.grid.get(i.colNames.get(j)).get(indexLocation.get(j)));
        }
        String finalpkDataType = pkDataType;
        Comparator<String> c1 = new Comparator<String>() {
            public int compare(String u1, String u2)
            {
                String u1Object = u1.split(",")[0];
                String u2Object = u2.split(",")[0];
                if(finalpkDataType.equals("java.util.Date")){
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                    try {
                        Date u1Date = formatter.parse(u1Object);
                        Date u2Date = formatter.parse(u2Object);
                        return compareTo(u1Date,u2Date);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                } else if(finalpkDataType.equals("java.lang.Integer")){
                    Integer u1Int = Integer.parseInt(u1Object);
                    Integer u2Int = Integer.parseInt(u2Object);
                    return compareTo(u1Int, u2Int);
                } else if(finalpkDataType.equals("java.lang.Double")){
                    Double u1Int = Double.parseDouble(u1Object);
                    Double u2Int = Double.parseDouble(u2Object);
                    return compareTo(u1Int, u2Int);
                }
                return compareTo(u1Object,u2Object);
            }
        };
        if(buckets.size() == 0){
            Bucket newB = new Bucket();
            newB.bucketVector.add(bucketRow);
            String bucketName = indexName.substring(0,indexName.length() - 4);
            for(int z = 0; z < indexLocation.size(); z++){
                bucketName+= indexLocation.get(z) + ",";
            }
            bucketName += "1,.ser";
            int iLoc = 0;
            for(String colName: i.colNames){
                i.grid.get(colName).get(indexLocation.get(iLoc)).add(bucketName);
                iLoc++;
            }
            saveBucket(newB, bucketName);
        } else {
            boolean done = false;
            for(int buckI = 0; buckI < buckets.size(); buckI++){
                if(done){
                    break;
                }
                Bucket b = loadBucket(buckets.get(buckI));
                int indexInBucket = Collections.binarySearch(b.bucketVector,pkValue, c1);
                indexInBucket = Math.abs(indexInBucket + 1);
                if(indexInBucket == b.bucketVector.size()){
                    if(buckI == (buckets.size() -1)){
                        b.bucketVector.add(indexInBucket,bucketRow);
                        if(b.bucketVector.size() > this.MaximumKeysCountinIndexBucket){
                            String lastRowStr = b.bucketVector.remove(b.bucketVector.size() - 1);
                            Bucket newB = new Bucket();
                            newB.bucketVector.add(lastRowStr);
                            String[] newBucketArr =  buckets.get(buckI).split(",");
                            String newBucketStr = "";
                            for(int newBucketI = 0; newBucketI < newBucketArr.length - 2; newBucketI++){
                                newBucketStr += newBucketArr[newBucketI] + ",";
                            }
                            String newBucketPtr = buckets.size() + "";
                            newBucketStr += newBucketPtr + ",.ser";
                            saveBucket(newB, newBucketStr);
                            int iLoc = 0;
                            for(String colName: i.colNames){
                                i.grid.get(colName).get(indexLocation.get(iLoc)).add(newBucketStr);
                                iLoc++;
                            }
                        }
                        saveBucket(b,buckets.get(buckI));
                        done = true;
                    } else {
                        continue;
                    }
                } else {
                    b.bucketVector.add(indexInBucket,bucketRow);
                    if(b.bucketVector.size() > this.MaximumKeysCountinIndexBucket){
                        String lastRowStr = b.bucketVector.remove(b.bucketVector.size() - 1);
                        try{
                            String nextBucketName = buckets.get(buckI+1);
                            Bucket nextB = loadBucket(nextBucketName);
                            if(nextB.bucketVector.size() == this.MaximumKeysCountinIndexBucket){
                                throw new IndexOutOfBoundsException();
                            }
                            nextB.bucketVector.add(0,lastRowStr);
                            saveBucket(nextB, nextBucketName);
                        } catch(IndexOutOfBoundsException e){
                            Bucket newB = new Bucket();
                            newB.bucketVector.add(lastRowStr);
                            String[] newBucketArr =  buckets.get(buckI).split(",");
                            String newBucketStr = "";
                            for(int newBucketI = 0; newBucketI < newBucketArr.length - 2; newBucketI++){
                                newBucketStr += newBucketArr[newBucketI] + ",";
                            }
                            String newBucketPtr = buckets.size() + "";
                            newBucketStr += newBucketPtr + ",.ser";
                            saveBucket(newB, newBucketStr);
                            int iLoc = 0;
                            for(String colName: i.colNames){
                                int indexInt = i.grid.get(colName).get(indexLocation.get(iLoc)).indexOf(buckets.get(buckI));
                                i.grid.get(colName).get(indexLocation.get(iLoc)).add(indexInt + 1,newBucketStr);
                                iLoc++;
                            }
                        }

                    }
                    saveBucket(b,buckets.get(buckI));
                    done = true;
                }
            }
        }
        return i;
    }

    public static int compareTo(Object pkValue, Object toBeCompared){
        if(pkValue instanceof Date){
            return ((Date)pkValue).compareTo((Date)toBeCompared) ;
        } else if(pkValue instanceof Integer){
            return ((Integer)pkValue).compareTo((Integer)toBeCompared) ;
        } else if(pkValue instanceof Double){
            return ((Double)pkValue).compareTo((Double) toBeCompared) ;
        }else{
            return ((String)pkValue).compareTo((String) toBeCompared) ;
        }
    }

    public void saveTable(Table t){
        //SAVE PAGE
        String pageLocation = "src/main/resources/data/" + t.tableName + ".ser";
        FileOutputStream fileOut =
                null;
        try {
            fileOut = new FileOutputStream(pageLocation);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(t);
            out.close();
            fileOut.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void savePage(Page p, String pageLocation){
        //SAVE PAGE
        FileOutputStream fileOut =
                null;
        try {
            fileOut = new FileOutputStream(pageLocation);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(p);
            out.close();
            fileOut.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //INCORRECT INPUT IN SOME COLUMNS NOT ALL??
    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
        Table t = null;
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + tableName + ".ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            t = (Table) in.readObject();
            in.close();
            fileIn.close();
        } catch (Exception e){
            e.printStackTrace();
            return;
        }
        String pkDataType = "";
        String pkColName = "";
        Hashtable<String,String> colInfo = new Hashtable<String,String>();
        File csvFile = new File("src/main/resources/metadata.csv");
        if (csvFile.isFile()) {
            BufferedReader csvReader = null;
            try {
                csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
                String row;
                while ((row = csvReader.readLine()) != null) {
                    String[] data = row.split(",");
                    // do something with the data
                    if(data[0].compareTo(tableName) == 0)    {
                        if(data[3].compareTo("True") == 0)     {
                            pkDataType = data[2];
                            pkColName = data[1];
                        } else {
                            String s = data[2]+"," + data[5]+"," + data[6];    //type min max
                            colInfo.put(data[1], s);
                        }
                    }
                }
                
                csvReader.close();


            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return;
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }

        }  else {
            throw new DBAppException("MetaData CSV not found");
        }
        Enumeration<String> en = columnNameValue.keys();
        while(en.hasMoreElements()){
            String colNameStr = en.nextElement();
            Object valueToBeInserted = columnNameValue.get(colNameStr);
            if(colInfo.containsKey(colNameStr))   {
               String[] colInfoExtract =  colInfo.get(colNameStr).split(",");
               switch (colInfoExtract[0]){
                   case "java.util.Date":
                       if(valueToBeInserted instanceof Date){
                           SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                           try {
                               Date dateMin = formatter.parse(colInfoExtract[1]);
                               Date dateMax = formatter.parse(colInfoExtract[2]);
                               if(((Date)valueToBeInserted).compareTo(dateMin) < 0 || ((Date)valueToBeInserted).compareTo(dateMax) > 0) {
                                   throw new DBAppException("Data out of bounds specified in Metadata file");
                               }
                           } catch (ParseException e) {
                               e.printStackTrace();
                               return;
                           }

                       }   else{
                           throw new DBAppException("Invalid data type input");
                       }
                       break;
                   case "java.lang.Integer":
                       if(valueToBeInserted instanceof Integer){
                           Integer intMin = Integer.parseInt(colInfoExtract[1]);
                           Integer intMax = Integer.parseInt(colInfoExtract[2]);
                           if(((Integer)valueToBeInserted).compareTo(intMin) < 0 || ((Integer)valueToBeInserted).compareTo(intMax) > 0) {
                               throw new DBAppException("Data out of bounds specified in Metadata file");
                           }
                       }   else{
                           throw new DBAppException("Invalid data type input");
                       }
                       break;
                   case "java.lang.Double":
                       if(valueToBeInserted instanceof Double){
                           Double dobMin = Double.parseDouble(colInfoExtract[1]);
                           Double dobMax = Double.parseDouble(colInfoExtract[2]);
                           if(((Double)valueToBeInserted).compareTo(dobMin) < 0 || ((Double)valueToBeInserted).compareTo(dobMax) > 0) {
                               throw new DBAppException("Data out of bounds specified in Metadata file");
                           }
                       }   else{
                           throw new DBAppException("Invalid data type input");
                       }
                       break;
                   case "java.lang.String":
                       if(valueToBeInserted instanceof String){
                           String strMin =   colInfoExtract[1];
                           String strMax =   colInfoExtract[2];
                           if(((String)valueToBeInserted).compareTo(strMin) < 0 || ((String)valueToBeInserted).compareTo(strMax) > 0) {
                               throw new DBAppException("Data out of bounds specified in Metadata file");
                           }
                       }  else{
                           throw new DBAppException("Invalid data type input");
                       }
                       break;
                   default: break;
               }
            } else {
                throw new DBAppException("Column input not found");
            }
        }
        boolean indexFound = false;
        String indexName = "";
        for(int indices = 0; indices < t.indices.size(); indices++){
            ArrayList<String> indexSplit = new ArrayList<String> (Arrays.asList(t.indices.get(indices).split(",")));
            if(indexSplit.contains(pkColName)){
                indexFound = true;
                indexName =  t.indices.get(indices);
                break;
            }
        }
        String targetPageStr = "";
        if(indexFound) {
            Index i = loadIndex(indexName);
            Object clusterKeyValueObject = null;
            if(pkDataType.equals("java.util.Date")){
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                try {
                    clusterKeyValueObject = formatter.parse(clusteringKeyValue);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            } else if(pkDataType.equals("java.lang.Double")){
                clusterKeyValueObject = Double.parseDouble(clusteringKeyValue);
            } else if(pkDataType.equals("java.lang.Integer")){
                clusterKeyValueObject = Integer.parseInt(clusteringKeyValue);
            }else{
               clusterKeyValueObject = clusteringKeyValue;
            }
            int indexLocation = this.binSearch(pkDataType, i.gridRange.get(pkColName), clusterKeyValueObject);
            ArrayList<String> buckets = i.grid.get(pkColName).get(indexLocation);

            if(buckets.size() != 0){
                String finalpkDataType = pkDataType;
                Comparator<String> c1 = new Comparator<String>() {
                    public int compare(String u1, String u2)
                    {
                        String u1Object = u1.split(",")[0];
                        String u2Object = u2.split(",")[0];
                        if(finalpkDataType.equals("java.util.Date")){
                            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                            try {
                                Date u1Date = formatter.parse(u1Object);
                                Date u2Date = formatter.parse(u2Object);
                                return compareTo(u1Date,u2Date);
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                        } else if(finalpkDataType.equals("java.lang.Integer")){
                            Integer u1Int = Integer.parseInt(u1Object);
                            Integer u2Int = Integer.parseInt(u2Object);
                            return compareTo(u1Int, u2Int);
                        } else if(finalpkDataType.equals("java.lang.Double")){
                            Double u1Int = Double.parseDouble(u1Object);
                            Double u2Int = Double.parseDouble(u2Object);
                            return compareTo(u1Int, u2Int);
                        }
                        return compareTo(u1Object,u2Object);
                    }
                };
                for(int buckI = 0; buckI < buckets.size(); buckI++){
                    Bucket b = loadBucket(buckets.get(buckI));
                    int indexInBucket = Collections.binarySearch(b.bucketVector,clusteringKeyValue, c1);
//                    System.out.println(indexInBucket);
                    if(indexInBucket >= 0){
                        targetPageStr = b.bucketVector.get(indexInBucket).split(",")[1];
                        break;
                    }
                    indexInBucket = Math.abs(indexInBucket + 1);
                    if(indexInBucket == b.bucketVector.size()){
                        targetPageStr = b.bucketVector.get(indexInBucket).split(",")[1];
                        continue;
                    } else {
                        throw new DBAppException("Clustering Key inserted not found!");
                    }
                }
            }
        }
        Page p = null;
        Page targetPage = null;
        String pageLocation = "";
        if(!(targetPageStr.equals(""))){
            pageLocation = "src/main/resources/data/" + targetPageStr;
            try {
                FileInputStream fileIn = new FileInputStream(pageLocation);
                ObjectInputStream in = new ObjectInputStream(fileIn);
                p = (Page) in.readObject();
                in.close();
                fileIn.close();
            } catch (Exception e){
                throw new DBAppException("Page not found!");
            }
            targetPage = p;
        }else {
            for(int i = 0; i < t.v.size(); i++){
                if(targetPage != null){
                    break;
                }
                pageLocation = "src/main/resources/data/" + t.v.get(i);
                try {
                    FileInputStream fileIn = new FileInputStream(pageLocation);
                    ObjectInputStream in = new ObjectInputStream(fileIn);
                    p = (Page) in.readObject();
                    in.close();
                    fileIn.close();
                } catch (Exception e){
                    throw new DBAppException("Page not found!");
                }
                switch(pkDataType)   {
                    case "java.lang.Integer":
                        int minI = (int) p.min;
                        int maxI = (int) p.max;
                        try {
                            int pkI = Integer.parseInt(clusteringKeyValue);
                            if(pkI <= maxI && pkI >= minI){
                                targetPage = p;
                            } else {
                                continue;
                            }
                        } catch (Exception e){
                            throw new DBAppException("Inserted clustering key type invalid");
                        }
                        break;
                    case "java.lang.Double":
                        double minD = (double) p.min;
                        double maxD = (double) p.max;
                        try {
                            double pkD = Double.parseDouble(clusteringKeyValue);
                            if(pkD <= maxD && pkD >= minD){
                                targetPage = p;
                            } else {
                                continue;
                            }
                        } catch (Exception e){
                            throw new DBAppException("Inserted clustering key type invalid");
                        }
                        break;
                    case "java.lang.String":
                        String minS = (String) p.min;
                        String maxS = (String) p.max;
                        try {
                            String pkS = clusteringKeyValue;
                            if(pkS.compareTo(minS) >= 0 && pkS.compareTo(maxS) <= 0){
                                targetPage = p;
                            } else {
                                continue;
                            }
                        } catch (Exception e){
                            throw new DBAppException("Inserted clustering key type invalid");
                        }
                        break;
                    case "java.util.Date":
                        Date minDate = (Date) p.min;
                        Date maxDate = (Date) p.max;
                        try {
                            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                            Date pkDate = formatter.parse(clusteringKeyValue);
                            if(pkDate.compareTo(minDate) >= 0 && pkDate.compareTo(maxDate) <= 0){
                                targetPage = p;
                            } else {
                                continue;
                            }
                        } catch (Exception e){
                            throw new DBAppException("Inserted clustering key type invalid");
                        }
                        break;
                    default: break;
                }
            }
        }

        if(targetPage == null){
            throw new DBAppException("Clustering Key inserted not found!");
        }
//        System.out.println(targetPageStr);
        String finalColName = pkColName;
        Comparator<Hashtable<String,Object>> c = new Comparator<Hashtable<String,Object>>() {
            public int compare(Hashtable<String,Object> u1, Hashtable<String,Object> u2)
            {
                Object u1Object = u1.get(finalColName);
                Object u2Object = u2.get(finalColName);
                if(u1Object instanceof Date){
                     return ((Date)u1Object).compareTo((Date)u2Object);
                } else if(u1Object instanceof String){
                    return ((String)u1Object).compareTo((String)u2Object);
                } else if(u1Object instanceof Integer){
                    return ((Integer)u1Object).compareTo((Integer)u2Object);
                } else {
                    return ((Double)u1Object).compareTo((Double)u2Object);
                }
            }
        };
        Hashtable<String, Object> h = new Hashtable<String, Object>();
        switch(pkDataType)   {
            case "java.lang.Integer":
                h.put(pkColName, Integer.parseInt(clusteringKeyValue));
                break;
            case "java.lang.Double":
                h.put(pkColName, Double.parseDouble(clusteringKeyValue));
                break;
            case "java.lang.String":
                h.put(pkColName, clusteringKeyValue);
                break;
            case "java.util.Date":
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                try {
                    Date pkDate = formatter.parse(clusteringKeyValue);
                    h.put(pkColName, pkDate);
                } catch (ParseException e) {
                    throw new DBAppException("Cannot parse date");
                }
                break;
            default: break;
        }
        int index = Collections.binarySearch(targetPage.page,h,c);
        if(index < 0 ){
            throw new DBAppException("Clustering Key Value not found to be updated");
        } else {
            Hashtable<String, Object> rowFound = targetPage.page.get(index);
            Hashtable<String, Object> rowFoundOld = targetPage.page.get(index);
            Enumeration<String> en1 = columnNameValue.keys();
            while(en1.hasMoreElements()) {
                String colNameStr = en1.nextElement();
                Object valueToBeInserted = columnNameValue.get(colNameStr);
                rowFound.replace(colNameStr, valueToBeInserted);
            }
            String bucketRow = clusteringKeyValue + "," + targetPageStr;
            colInfo.put(pkColName,pkDataType);
            for(int tableIndex = 0; tableIndex < t.indices.size(); tableIndex++){
                Index i = loadIndex(t.indices.get(tableIndex));
                i = deleteFromIndex(i, t.indices.get(tableIndex), colInfo, rowFoundOld, bucketRow, clusteringKeyValue, pkDataType);
                i = insertIntoIndex(i, t.indices.get(tableIndex), colInfo, rowFound, bucketRow,clusteringKeyValue,pkDataType);
                try {
                    saveIndex(i, t.indices.get(tableIndex));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            savePage(targetPage, pageLocation);
            saveTable(t);

        }
    }

    public Index deleteFromIndex(Index i, String indexName, Hashtable<String, String> colInfo, Hashtable<String, Object> rowFoundOld, String bucketRow, String clusteringKeyValue, String pkDataType) {
        //Remove bucket if empty
        ArrayList<Integer> indexLocation = new ArrayList<Integer>();
        for(String colName: i.colNames){
            String dataType = colInfo.get(colName).split(",")[0];
            indexLocation.add(this.binSearch(dataType, i.gridRange.get(colName), rowFoundOld.get(colName)));
        }
        ArrayList<String> buckets = new ArrayList<String>(i.grid.get(i.colNames.get(0)).get(indexLocation.get(0)));
        for(int j = 1; j < i.colNames.size(); j++){
            buckets.retainAll(i.grid.get(i.colNames.get(j)).get(indexLocation.get(j)));
        }
        String finalpkDataType = pkDataType;
        Comparator<String> c1 = new Comparator<String>() {
            public int compare(String u1, String u2)
            {
                String u1Object = u1.split(",")[0];
                String u2Object = u2.split(",")[0];
                if(finalpkDataType.equals("java.util.Date")){
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                    try {
                        Date u1Date = formatter.parse(u1Object);
                        Date u2Date = formatter.parse(u2Object);
                        return compareTo(u1Date,u2Date);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                } else if(finalpkDataType.equals("java.lang.Integer")){
                    Integer u1Int = Integer.parseInt(u1Object);
                    Integer u2Int = Integer.parseInt(u2Object);
                    return compareTo(u1Int, u2Int);
                } else if(finalpkDataType.equals("java.lang.Double")){
                    Double u1Int = Double.parseDouble(u1Object);
                    Double u2Int = Double.parseDouble(u2Object);
                    return compareTo(u1Int, u2Int);
                }
                return compareTo(u1Object,u2Object);
            }
        };
        boolean done = false;
        for(int buckI = 0; buckI < buckets.size(); buckI++){
            if(done){
                break;
            }
            Bucket b = loadBucket(buckets.get(buckI));
            String pkValue = bucketRow.split(",")[0] + "";
            if(pkDataType.equals("java.util.Date")){
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
//                System.out.println(pkValue);
                try {
                    pkValue = formatter.format(formatter.parse(pkValue));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
            int indexInBucket = Collections.binarySearch(b.bucketVector,pkValue, c1);
            if(indexInBucket < 0){
                continue;
            } else {
                b.bucketVector.remove(indexInBucket);
                if(b.bucketVector.size() == 0){
                   File toBeDeleted = new File("src/main/resources/data/" + buckets.get(buckI));
                   toBeDeleted.delete();
                   int indexRemove = 0;
                   String bucketToBeRemoved = buckets.get(buckI);
                   for(String colName: i.colNames){
                       try{
                           i.grid.get(colName).get(indexLocation.get(indexRemove)).remove(bucketToBeRemoved);
                           indexRemove++;
                       }  catch(Exception e){
                           System.out.println(indexLocation);
                           System.out.println(i.colNames);
                           System.out.println(buckets);
                           System.out.println(i.grid);
                           return null;
                       }

                   }
                }  else{
                    saveBucket(b,buckets.get(buckI));
                }
                done = true;
            }
        }
        return i;
    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        Table t = null;
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + tableName + ".ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            t = (Table) in.readObject();
            in.close();
            fileIn.close();
        } catch (Exception e){
            throw new DBAppException("Table not found!");
        }
        File csvFile = new File("src/main/resources/metadata.csv");
        Hashtable<String, String> colNameType = new Hashtable<String, String>();
        String pkDataType = "";
        String pkColName = "";
        if (csvFile.isFile()) {
            BufferedReader csvReader = null;
            try {
                csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
                String row;
                while ((row = csvReader.readLine()) != null) {
                    String[] data = row.split(",");
                    if(data[0].compareTo(tableName) == 0){
                        if(data[3].compareTo("True") == 0)     {
                            pkDataType = data[2];
                            pkColName = data[1];
                        }
                        colNameType.put(data[1],data[2]);
                    }

                }

                csvReader.close();


            } catch (FileNotFoundException e) {
                throw new DBAppException("CSV not found");
            } catch (IOException e) {
                throw new DBAppException("CSV not found");
            }
            Enumeration<String> en = columnNameValue.keys();
            while(en.hasMoreElements()){
                String colNameStr = en.nextElement();
                Object valueToBeDeleted = columnNameValue.get(colNameStr);
                if(colNameType.containsKey(colNameStr))   {
                    String colType =  colNameType.get(colNameStr);
                    switch (colType){
                        case "java.util.Date":
                            if(!(valueToBeDeleted instanceof Date)){
                                throw new DBAppException("Invalid data type input");
                            }
                            break;
                        case "java.lang.Integer":
                            if(!(valueToBeDeleted instanceof Integer)){
                                throw new DBAppException("Invalid data type input");
                            }
                            break;
                        case "java.lang.Double":
                            if(!(valueToBeDeleted instanceof Double)){
                                throw new DBAppException("Invalid data type input");
                            }
                            break;
                        case "java.lang.String":
                            if(!(valueToBeDeleted instanceof String)){
                                throw new DBAppException("Invalid data type input");
                            }
                            break;
                        default: break;
                    }
                } else {
                    throw new DBAppException("Column input not found");
                }
            }
            ArrayList<String> inputCols = new ArrayList<String>(columnNameValue.keySet());
            String indexToBeUsed = null;
            int indexPriority = 0;
            for(int ind = 0; ind < t.indices.size(); ind++){
                ArrayList<String> indexArr = new ArrayList<String>(Arrays.asList(t.indices.get(ind).split(",")));
                indexArr.remove(0);
                indexArr.remove(indexArr.size() - 1);
                int oldSize = indexArr.size();
                indexArr.retainAll(inputCols);
                if(indexArr.size() == inputCols.size()){
                    indexToBeUsed = t.indices.get(ind);
                    break;
                } else if(indexArr.size() > indexPriority){
                    indexToBeUsed = t.indices.get(ind);
                    indexPriority = indexArr.size();
                }
            }
            Vector<Hashtable<String,Object>> rowsToBeDeleted = new Vector<Hashtable<String,Object>>();
            if(indexToBeUsed == null){
                Page p = null;
                for(int i = 0; i < t.v.size(); i++){
                    //Page Deserialization
                    String pageLocation = "src/main/resources/data/" + t.v.get(i);

                    try {
                        FileInputStream fileIn = new FileInputStream(pageLocation);
                        ObjectInputStream in = new ObjectInputStream(fileIn);
                        p = (Page) in.readObject();
                        in.close();
                        fileIn.close();
                    } catch (Exception e){
                        System.out.println(pageLocation);
                        throw new DBAppException("Page not found!");
                    }

                    for(int j = 0; j<p.page.size(); j++){
                        Hashtable<String, Object> row = p.page.get(j);
                        Enumeration<String> enCheck = columnNameValue.keys();
                        boolean deleteRow = true;
                        while(enCheck.hasMoreElements()){
                            if(!deleteRow){
                                break;
                            }
                            String colNameStr = enCheck.nextElement();
                            Object valueToBeDeleted = columnNameValue.get(colNameStr);
                            if(valueToBeDeleted instanceof String){
                                String valueToBeDeletedTypeCast =  (String) valueToBeDeleted;
                                if(((String)row.get(colNameStr)).compareTo(valueToBeDeletedTypeCast) != 0){
                                    deleteRow = false;
                                }
                            } else if(valueToBeDeleted instanceof Double){
                                Double valueToBeDeletedTypeCast =  (Double) valueToBeDeleted;
                                if(((Double)row.get(colNameStr)).compareTo(valueToBeDeletedTypeCast) != 0){
                                    deleteRow = false;
                                }
                            } else if(valueToBeDeleted instanceof Integer){
                                Integer valueToBeDeletedTypeCast =  (Integer) valueToBeDeleted;
                                if(((Integer)row.get(colNameStr)).compareTo(valueToBeDeletedTypeCast) != 0){
                                    deleteRow = false;
                                }
                            } else if(valueToBeDeleted instanceof Date){
                                Date valueToBeDeletedTypeCast =  (Date) valueToBeDeleted;
                                if(((Date)row.get(colNameStr)).compareTo(valueToBeDeletedTypeCast) != 0){
                                    deleteRow = false;
                                }
                            }
                        }
                        if(deleteRow){
                            rowsToBeDeleted.add(p.page.remove(j));
                            j--;
                        }
                    }
                    if(p.page.size()  == 0){
                        File pageToBeDeleted = new File(pageLocation);
                        pageToBeDeleted.delete();
                        t.v.remove(i);
                        i--;
                    } else{
                        savePage(p,pageLocation);
                    }
                }
            } else {
                Index i = loadIndex(indexToBeUsed);
                ArrayList<Integer> indexLocation = new ArrayList<Integer>();
                ArrayList<String> validColumns = new ArrayList<String>();
                for(String colName: i.colNames){
                    Object targetObject = columnNameValue.get(colName);
                    if(targetObject != null){
                        validColumns.add(colName);
                        String dataType = colNameType.get(colName).split(",")[0];
                        indexLocation.add(this.binSearch(dataType, i.gridRange.get(colName), targetObject));
                    }
                }
                ArrayList<String> buckets = new ArrayList<String>(i.grid.get(validColumns.get(0)).get(indexLocation.get(0)));
                for(int j = 1; j < validColumns.size(); j++){
                    buckets.retainAll(i.grid.get(validColumns.get(j)).get(indexLocation.get(j)));
                }
                Hashtable<String, ArrayList<String>> pageToClusterKey = new Hashtable<String, ArrayList<String>>();
                for(int bucketIndex = 0; bucketIndex < buckets.size(); bucketIndex++){
                    Bucket b = loadBucket(buckets.get(bucketIndex));
                    for(String bucketRow : b.bucketVector){
                        String[] bucketRowSplit = bucketRow.split(",");
                        if(pageToClusterKey.get(bucketRowSplit[1]) == null){
                            pageToClusterKey.put(bucketRowSplit[1], new ArrayList<String>());
                        }
                        pageToClusterKey.get(bucketRowSplit[1]).add(bucketRowSplit[0]);
                    }
                }
                ArrayList<String> listKeys = new ArrayList<String>( pageToClusterKey.keySet() );
                for(String pageName : listKeys){
                    String pageLocation = "src/main/resources/data/" + pageName;
                    Page p = null;
                    try {
                        FileInputStream fileIn = new FileInputStream(pageLocation);
                        ObjectInputStream in = new ObjectInputStream(fileIn);
                        p = (Page) in.readObject();
                        in.close();
                        fileIn.close();
                    } catch (Exception e){
                        System.out.println(pageLocation);
                        throw new DBAppException("Page not found!");
                    }
                    String finalColName = pkColName;
                    Comparator<Hashtable<String,Object>> c = new Comparator<Hashtable<String,Object>>() {
                        public int compare(Hashtable<String,Object> u1, Hashtable<String,Object> u2)
                        {
                            Object u1Object = u1.get(finalColName);
                            Object u2Object = u2.get(finalColName);
                            if(u1Object instanceof Date){
                                return ((Date)u1Object).compareTo((Date)u2Object);
                            } else if(u1Object instanceof String){
                                return ((String)u1Object).compareTo((String)u2Object);
                            } else if(u1Object instanceof Integer){
                                return ((Integer)u1Object).compareTo((Integer)u2Object);
                            } else {
                                return ((Double)u1Object).compareTo((Double)u2Object);
                            }
                        }
                    };
                    for(String clusterKey : pageToClusterKey.get(pageName)){
                        Hashtable<String,Object> h = new Hashtable<String,Object>();
                        Object clusterKeyObject = null;
                        if(pkDataType.equals("java.util.Date")){
                            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                            try {
                                clusterKeyObject = formatter.parse(clusterKey);
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                        } else if(pkDataType.equals("java.lang.Integer")){
                            clusterKeyObject = Integer.parseInt(clusterKey);
                        } else if(pkDataType.equals("java.lang.Double")){
                            clusterKeyObject = Double.parseDouble(clusterKey);
                        } else {
                            clusterKeyObject = clusterKey;
                        }
                        h.put(pkColName,clusterKeyObject);
                        int indexInPage =  Collections.binarySearch(p.page,h,c);
                        Hashtable<String, Object> row = p.page.get(indexInPage);
                        Enumeration<String> enCheck = columnNameValue.keys();
                        boolean deleteRow = true;
                        while(enCheck.hasMoreElements()){
                            if(!deleteRow){
                                break;
                            }
                            String colNameStr = enCheck.nextElement();
                            Object valueToBeDeleted = columnNameValue.get(colNameStr);
                            if(valueToBeDeleted instanceof String){
                                String valueToBeDeletedTypeCast =  (String) valueToBeDeleted;
                                if(((String)row.get(colNameStr)).compareTo(valueToBeDeletedTypeCast) != 0){
                                    deleteRow = false;
                                }
                            } else if(valueToBeDeleted instanceof Double){
                                Double valueToBeDeletedTypeCast =  (Double) valueToBeDeleted;
                                if(((Double)row.get(colNameStr)).compareTo(valueToBeDeletedTypeCast) != 0){
                                    deleteRow = false;
                                }
                            } else if(valueToBeDeleted instanceof Integer){
                                Integer valueToBeDeletedTypeCast =  (Integer) valueToBeDeleted;
                                if(((Integer)row.get(colNameStr)).compareTo(valueToBeDeletedTypeCast) != 0){
                                    deleteRow = false;
                                }
                            } else if(valueToBeDeleted instanceof Date){
                                Date valueToBeDeletedTypeCast =  (Date) valueToBeDeleted;
                                if(((Date)row.get(colNameStr)).compareTo(valueToBeDeletedTypeCast) != 0){
                                    deleteRow = false;
                                }
                            }
                        }
                        if(deleteRow){
                            p.page.remove(indexInPage);
                            rowsToBeDeleted.add(row);
                        }
                        if(p.page.size()  == 0){
                            File pageToBeDeleted = new File(pageLocation);
                            pageToBeDeleted.delete();
                            t.v.remove(pageName);
                        } else{
                            savePage(p,pageLocation);
                        }
                    }
                }

            }

            for(int tableIndex = 0; tableIndex<t.indices.size(); tableIndex++){
                Index i = loadIndex(t.indices.get(tableIndex));
                for(Hashtable<String,Object> row: rowsToBeDeleted){
                    String clusterKeyStr = row.get(pkColName) + "";
                    if(pkDataType.equals("java.util.Date")){
                        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                        clusterKeyStr = formatter.format(row.get(pkColName));
                    }
                    i = deleteFromIndex(i,t.indices.get(tableIndex),colNameType,row,clusterKeyStr,clusterKeyStr,pkDataType);
                }
                try {
                    saveIndex(i,t.indices.get(tableIndex));
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new DBAppException("Cannot save index");
                }
            }
            saveTable(t);
        }  else {
            throw new DBAppException("MetaData CSV not found");
        }
    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        HashSet<Hashtable<String,Object>> res = new HashSet<Hashtable<String,Object>>();
        if(sqlTerms.length == 0) {
            throw new DBAppException("Please enter full SQL Term");
        }
        String tableName = sqlTerms[0]._strTableName;
        Table t = loadTable(tableName);
        String pkDataType = "";
        String pkColName = "";
        Hashtable<String,String> colInfo = new Hashtable<String,String>();
        File csvFile = new File("src/main/resources/metadata.csv");
        BufferedReader csvReader = null;
        try {
            csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            String row;
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                // do something with the data
                if(data[0].compareTo(tableName) == 0)    {
                    if(data[3].compareTo("True") == 0)     {
                        pkDataType = data[2];
                        pkColName = data[1];
                    }

                    colInfo.put(data[1], data[2]);
                }
            }
            csvReader.close();
        } catch (Exception e){
            e.printStackTrace();
        }
        String finalColName = pkColName;
        Comparator<Hashtable<String,Object>> c = new Comparator<Hashtable<String,Object>>() {
            public int compare(Hashtable<String,Object> u1, Hashtable<String,Object> u2)
            {
                Object u1Object = u1.get(finalColName);
                Object u2Object = u2.get(finalColName);
                if(u1Object instanceof Date){
                    return ((Date)u1Object).compareTo((Date)u2Object);
                } else if(u1Object instanceof String){
                    return ((String)u1Object).compareTo((String)u2Object);
                } else if(u1Object instanceof Integer){
                    return ((Integer)u1Object).compareTo((Integer)u2Object);
                } else {
                    return ((Double)u1Object).compareTo((Double)u2Object);
                }
            }
        };
        Hashtable<String,String> isThereAnIndex = new Hashtable<String,String>();
        for(int i = 0; i < sqlTerms.length; i++) {
            if(!tableName.equals(sqlTerms[i]._strTableName)){
                throw new DBAppException("Please enter a single table in the SQL Terms");
            }
            for(String indexName: t.indices){
                if(new ArrayList<String>(Arrays.asList(indexName.split(","))).contains(sqlTerms[i]._strColumnName)){
                    isThereAnIndex.put(sqlTerms[i]._strColumnName,indexName);
                    break;
                }
            }
        }
        Vector<Vector<Hashtable<String,Object>>> intermediate = new Vector<Vector<Hashtable<String,Object>>>();
        int intermediateInt = -1;
        for(SQLTerm sqlterm : sqlTerms) {
            intermediateInt++;
            intermediate.add(new Vector<Hashtable<String,Object>>());
            boolean indexUsed = false;
            Object o = sqlterm._objValue;
            if(isThereAnIndex.containsKey(sqlterm._strColumnName)){
                indexUsed = true;
                Index i = loadIndex(isThereAnIndex.get(sqlterm._strColumnName));
                Hashtable<String, Vector<Object>> pageToRow = new  Hashtable<String, Vector<Object>>();
                if(sqlterm._strOperator.equals(">") || sqlterm._strOperator.equals(">=")){
                    int indexLocGrt = 0;
                    try{
                        indexLocGrt = this.binSearch(colInfo.get(sqlterm._strColumnName),i.gridRange.get(sqlterm._strColumnName),o);
                    } catch(Exception e){
                        indexLocGrt = 10;
                    }
                    for(int j = indexLocGrt; j < 10; j++){
                        ArrayList<String> buckets = i.grid.get(sqlterm._strColumnName).get(j);
                        for(String bucket : buckets){
                            Bucket b = loadBucket(bucket);
                            for(String bucketRow: b.bucketVector){
                                String[] bucketRowSplit = bucketRow.split(",");
                                Object clusterKeyObject = null;
                                if(pkDataType.equals("java.util.Date")){
                                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                                    try {
                                        clusterKeyObject = formatter.parse(bucketRowSplit[0]);
                                    } catch (ParseException e) {
                                        e.printStackTrace();
                                    }
                                } else if(pkDataType.equals("java.lang.Integer")){
                                    clusterKeyObject = Integer.parseInt(bucketRowSplit[0]);
                                } else if(pkDataType.equals("java.lang.Double")){
                                    clusterKeyObject = Double.parseDouble(bucketRowSplit[0]);
                                } else {
                                    clusterKeyObject = bucketRowSplit[0];
                                }
                                if(pageToRow.get(bucketRowSplit[1]) == null){
                                    pageToRow.put(bucketRowSplit[1], new Vector<Object>());
                                }
                                pageToRow.get(bucketRowSplit[1]).add(clusterKeyObject);
                            }
                        }
                    }
                    for(String pageName : pageToRow.keySet()){
                        Page p = loadPage(pageName);
                        for(Object pkValue : pageToRow.get(pageName)){
                            Hashtable<String, Object> pkValueHashtable = new Hashtable<String, Object>();
                            pkValueHashtable.put(pkColName,pkValue);
                            Hashtable<String, Object> row = p.page.get(Collections.binarySearch(p.page,pkValueHashtable,c));
                            if(sqlterm._strOperator.equals(">")){
                                if(compareTo(row.get(sqlterm._strColumnName),o) > 0){
                                    intermediate.get(intermediateInt).add(row);
                                }
                            }else {
                                if(compareTo(row.get(sqlterm._strColumnName),o) >= 0){
                                    intermediate.get(intermediateInt).add(row);
                                }
                            }
                        }
                    }
                } else if(sqlterm._strOperator.equals("<") || sqlterm._strOperator.equals("<=")){
                    int indexLocLess = 9;
                    try{
                        indexLocLess = this.binSearch(colInfo.get(sqlterm._strColumnName),i.gridRange.get(sqlterm._strColumnName),o);
                    } catch(Exception e){
                        indexLocLess = -1;
                    }
                    for(int j = 0; j <= indexLocLess; j++){
                        ArrayList<String> buckets = i.grid.get(sqlterm._strColumnName).get(j);
                        for(String bucket : buckets){
                            Bucket b = loadBucket(bucket);
                            for(String bucketRow: b.bucketVector){
                                String[] bucketRowSplit = bucketRow.split(",");
                                Object clusterKeyObject = null;
                                if(pkDataType.equals("java.util.Date")){
                                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                                    try {
                                        clusterKeyObject = formatter.parse(bucketRowSplit[0]);
                                    } catch (ParseException e) {
                                        e.printStackTrace();
                                    }
                                } else if(pkDataType.equals("java.lang.Integer")){
                                    clusterKeyObject = Integer.parseInt(bucketRowSplit[0]);
                                } else if(pkDataType.equals("java.lang.Double")){
                                    clusterKeyObject = Double.parseDouble(bucketRowSplit[0]);
                                } else {
                                    clusterKeyObject = bucketRowSplit[0];
                                }
                                if(pageToRow.get(bucketRowSplit[1]) == null){
                                    pageToRow.put(bucketRowSplit[1], new Vector<Object>());
                                }
                                pageToRow.get(bucketRowSplit[1]).add(clusterKeyObject);
                            }
                        }
                    }
                    for(String pageName : pageToRow.keySet()){
                        Page p = loadPage(pageName);
                        for(Object pkValue : pageToRow.get(pageName)){
                            Hashtable<String, Object> pkValueHashtable = new Hashtable<String, Object>();
                            pkValueHashtable.put(pkColName,pkValue);
                            Hashtable<String, Object> row = p.page.get(Collections.binarySearch(p.page,pkValueHashtable,c));
                            if(sqlterm._strOperator.equals("<")){
                                if(compareTo(row.get(sqlterm._strColumnName),o) < 0){
                                    intermediate.get(intermediateInt).add(row);
                                }
                            }else {
                                if(compareTo(row.get(sqlterm._strColumnName),o) <= 0){
                                    intermediate.get(intermediateInt).add(row);
                                }
                            }
                        }
                    }
                } else if(sqlterm._strOperator.equals("!=")){
                    indexUsed = false;
                } else if(sqlterm._strOperator.equals("=")){
                    int indexLocEql = 0;
                    ArrayList<String> buckets = new ArrayList<String>();
                    try{
                        indexLocEql= this.binSearch(colInfo.get(sqlterm._strColumnName),i.gridRange.get(sqlterm._strColumnName),o);
                        buckets = i.grid.get(sqlterm._strColumnName).get(indexLocEql);
                    } catch(Exception e){
                        indexLocEql = 0;
                        buckets = new ArrayList<String>();
                    }
                    for(String bucket : buckets){
                        Bucket b = loadBucket(bucket);
                        for(String bucketRow: b.bucketVector){
                            String[] bucketRowSplit = bucketRow.split(",");
                            Object clusterKeyObject = null;
                            if(pkDataType.equals("java.util.Date")){
                                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                                try {
                                    clusterKeyObject = formatter.parse(bucketRowSplit[0]);
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                }
                            } else if(pkDataType.equals("java.lang.Integer")){
                                clusterKeyObject = Integer.parseInt(bucketRowSplit[0]);
                            } else if(pkDataType.equals("java.lang.Double")){
                                clusterKeyObject = Double.parseDouble(bucketRowSplit[0]);
                            } else {
                                clusterKeyObject = bucketRowSplit[0];
                            }
                            if(pageToRow.get(bucketRowSplit[1]) == null){
                                pageToRow.put(bucketRowSplit[1], new Vector<Object>());
                            }
                            pageToRow.get(bucketRowSplit[1]).add(clusterKeyObject);
                        }
                    }
                    for(String pageName : pageToRow.keySet()){
                        Page p = loadPage(pageName);
                        for(Object pkValue : pageToRow.get(pageName)){
                            Hashtable<String, Object> pkValueHashtable = new Hashtable<String, Object>();
                            pkValueHashtable.put(pkColName,pkValue);
                            Hashtable<String, Object> row = p.page.get(Collections.binarySearch(p.page,pkValueHashtable,c));
                            if(compareTo(row.get(sqlterm._strColumnName),o) == 0){
                                intermediate.get(intermediateInt).add(row);
                            }
                        }
                    }
                } else {
                    throw new DBAppException("Unsupported operator");
                }
            }
            if(!indexUsed){
               for(String pageName: t.v){
                   Page p = loadPage(pageName);
                   for(Hashtable<String, Object> row: p.page){
                       int resOfCompareTo = compareTo(row.get(sqlterm._strColumnName),o);
                       switch (sqlterm._strOperator) {
                           case "=":
                               if(resOfCompareTo == 0){
                                  intermediate.get(intermediateInt).add(row);
                               }
                               break;
                           case "!=":
                               if(resOfCompareTo != 0){
                                   intermediate.get(intermediateInt).add(row);
                               }
                               break;
                           case ">=":
                               if(resOfCompareTo >= 0){
                                   intermediate.get(intermediateInt).add(row);
                               }
                               break;
                           case ">":
                               if(resOfCompareTo > 0){
                                   intermediate.get(intermediateInt).add(row);
                               }
                               break;
                           case "<":
                               if(resOfCompareTo < 0){
                                   intermediate.get(intermediateInt).add(row);
                               }
                               break;
                           case "<=":
                               if(resOfCompareTo <= 0){
                                   intermediate.get(intermediateInt).add(row);
                               }
                               break;
                           default:
                               throw new DBAppException("Unsupported operator");
                       }
                   }
               }
            }
        }
//        System.out.println(intermediate.get(0));
//        System.out.println(intermediate.get(1));

        res.addAll(intermediate.get(0));
        int i = 1;
        for(String arrOpp : arrayOperators){
           switch (arrOpp){
               case "AND":
                   res.retainAll(intermediate.get(i));
                   break;
               case "OR":
                   res.addAll(intermediate.get(i));
                   break;
               case "XOR":
                   HashSet<Hashtable<String,Object>> newRes = new HashSet<Hashtable<String,Object>>();
                   newRes.addAll(res);
                   newRes.addAll(intermediate.get(i));
                   res.retainAll(intermediate.get(i));
                   newRes.removeAll(res);
                   res.clear();
                   res.addAll(newRes);
                   break;
               default: throw new DBAppException("Unsupported Array Operator");
           }
           i++;
        }
        return res.iterator();
    }

    public static void main(String[] args){
//        DBApp a = new DBApp();
//        String tableLocation = "src/main/resources/data/students.ser";
//        Table t = null;
//        try {
//            FileInputStream fileIn = new FileInputStream(tableLocation);
//            ObjectInputStream in = new ObjectInputStream(fileIn);
//            t = (Table) in.readObject();
//            in.close();
//            fileIn.close();
//        } catch (Exception e){
//            System.out.println(tableLocation);
//        }
//        System.out.println(t.indices.get(0));
//            String pageLocation = "src/main/resources/data/" + t.indices.get(0);
//            Index p = null;
//            try {
//                FileInputStream fileIn = new FileInputStream(pageLocation);
//                ObjectInputStream in = new ObjectInputStream(fileIn);
//                p = (Index) in.readObject();
//                in.close();
//                fileIn.close();
//            } catch (Exception e){
//                System.out.println(pageLocation);
//            }
//            System.out.println(p.grid.get("id"));
//             for(int j = 0; j<p.grid.get("id").size(); j++){
//                 Bucket b = a.loadBucket(p.grid.get("id").get(j).get(0));
//                  System.out.println(b.bucketVector);
//             }
//        for(int i = 0; i < t.v.size(); i++){
//            String pageLocation = "src/main/resources/data/" + t.v.get(i);
//            Page p = null;
//            try {
//                FileInputStream fileIn = new FileInputStream(pageLocation);
//                ObjectInputStream in = new ObjectInputStream(fileIn);
//                p = (Page) in.readObject();
//                in.close();
//                fileIn.close();
//            } catch (Exception e){
//                System.out.println(pageLocation);
//            }
//            System.out.println(t.v.get(i));
//            for(int j = 0; j<p.page.size(); j++){
//                System.out.println(p.page.get(j));
//            }
//        }
        //Test binary search and getRange
//        DBApp db = new DBApp();
//        String min = "000";
//        String max = "999";
//        Hashtable<String, String> h = new Hashtable<String,String>();
//        h.put("Name","java.util.Date,1999-12-12,2001-12-31");
//        Hashtable<String, ArrayList<String>> h1 = db.getRange(h, new String[]{"Name"});
//        System.out.println(h1);
//        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
//        Date d = formatter.parse("2000-12-12");
//        System.out.println(db.binSearch("java.util.Date", h1.get("Name"), d));
//        Vector<Integer> a = new Vector<Integer>();
//        a.add(1);
//        a.add(2);
//        a.add(3);
//        System.out.println(a);
    }
}
