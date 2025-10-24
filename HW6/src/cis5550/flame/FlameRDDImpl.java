package cis5550.flame;
import java.util.ArrayList;
import java.util.List;
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


}
