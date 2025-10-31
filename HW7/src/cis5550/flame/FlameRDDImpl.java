package cis5550.flame;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import cis5550.kvs.Row;
import cis5550.tools.Serializer;

import java.util.Iterator;

import cis5550.kvs.KVSClient;

public class FlameRDDImpl implements FlameRDD {
    String tablename;
    //KVSClient kvs;
    FlameContextImpl context;
    public FlameRDDImpl (String tablename, FlameContextImpl context){
        this.tablename=tablename;
        this.context=context;
    }
    public List<String> collect() throws Exception{
        //gotta start off making a list to hold the results, then scan the table, then go through all the rows and find the string from the value col and hten add that to the list
        List<String> result=new ArrayList<>();
        KVSClient onekvs=context.getKVS();
        if(onekvs==null){
            throw new Exception("kvs client is not there");
        }
        Iterator<Row> rows=onekvs.scan(tablename, null, null);
        while(rows.hasNext()){
            Row r=rows.next(); 
            String value=r.get("value"); //getting the value from the row
            if (value!=null){
                result.add(value); //adding it to the list
            }
        }
        
        return result;
    }
    public FlameRDD flatMap(StringToIterable lambda) throws Exception{
        //basically serilaization the function into bytes and then call th einvokeoperation helper by giving the name, the worker route and the serialized lambda
        byte[] serlambda=Serializer.objectToByteArray(lambda);
        String outputtablename=context.invokeOperation(this.tablename, "/rdd/flatMap", serlambda, null);
        FlameRDDImpl answer=new FlameRDDImpl(outputtablename, this.context);
        //im returnign the flamerddimpl pointing to the results table
        return answer;
    }
    public FlamePairRDD mapToPair(StringToPair lambda) throws Exception{
        byte[] serlambda=Serializer.objectToByteArray(lambda);
        String otname=context.invokeOperation(this.tablename,"/rdd/mapToPair", serlambda, null);
        FlamePairRDDImpl answer=new FlamePairRDDImpl(otname, this.context); //returning the flameparirddimpl pointing to th eoutput table
        return answer;
    }
    public FlameRDD intersection(FlameRDD r) throws Exception{
        return null;
    }
    public FlameRDD sample(double f) throws Exception{
        return null;
    }
    public FlamePairRDD groupBy(StringToString lambda) throws Exception{
        return null;
    }
    public int count() throws Exception{
        long totalrows=context.getKVS().count(this.tablename);
        //gotta conver itt to an int
        int introws=(int) totalrows;
        return introws;
    }
    public void saveAsTable(String tablenameee) throws Exception{
        context.getKVS().rename(this.tablename, tablenameee);
        this.tablename=tablenameee; //updating the rddd

    }
    public FlameRDD distinct() throws Exception{
        String outputtablename=context.invokeOperation(this.tablename, "/rdd/distinct", null, null);
        FlameRDD answer=new FlameRDDImpl(outputtablename, this.context);
        return answer;
    }
    public void destroy() throws Exception{

    }

    public Vector<String> take(int num) throws Exception{
        Vector<String> result= new Vector<>();
        KVSClient kvs=context.getKVS();
        Iterator<Row> rows=kvs.scan(this.tablename, null, null); //basically scanning table form thebeginning
        while(rows.hasNext() &&result.size()<num){ //gotta loop until run out of rows or have num eelemtns
            Row row=rows.next();
            String v=row.get("value");
            if(v!=null){
                result.add(v);
            }
        }
        return result;
    }
    public String fold(String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception{
        byte[] bytelambda=Serializer.objectToByteArray(lambda);
        return context.invokeFoldOperation(this.tablename, "/rdd/fold", bytelambda, zeroElement);
    }

    public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception{
        byte[] bytelambda=Serializer.objectToByteArray(lambda);
        String outputtablename=context.invokeOperation(this.tablename, "/rdd/flatMapToPair", bytelambda, null);
        FlamePairRDD answer=new FlamePairRDDImpl(outputtablename, this.context);
        return answer;
    }
    public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception{
        return null;
    }
    @Override
    public FlameRDD filter(StringToBoolean lambda) throws Exception{
        return null;
    }


}
