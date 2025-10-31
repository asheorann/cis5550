package cis5550.flame;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;
public class FlamePairRDDImpl implements FlamePairRDD{
    String tablename;
    FlameContextImpl context;
    public FlamePairRDDImpl(String tablename, FlameContextImpl context){
        this.tablename=tablename;
        this.context=context;
    }
    public List<FlamePair> collect() throws Exception{
         //gotta start off making a list to hold the results, then scan the table, then go through all the rows and find the string from the value col and hten add that to the list
        List<FlamePair> result=new ArrayList<>();
        KVSClient onekvs=context.getKVS();
        if(onekvs==null){
            throw new Exception("kvs client is not there");
        }
        Iterator<Row> rows=onekvs.scan(this.tablename, null, null);
        while(rows.hasNext()){
            Row r=rows.next(); 
            String krkey=r.key();
            Set<String> colnames=r.columns();
            for(String c: colnames){
                String value=r.get(c); //getting the value from the row
                if (value!=null){
                    FlamePair needtoadd= new FlamePair(krkey, value);
                    result.add(needtoadd); //adding it to the list
                }   
            }
        }
        
        return result;
    }

	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception{
        byte[] serlambda=Serializer.objectToByteArray(lambda);
        String otname=context.invokeOperation(tablename, "/pair/foldByKey", serlambda, zeroElement);
        FlamePairRDDImpl answer= new FlamePairRDDImpl(otname, this.context);
         //print(answer);
        return answer;
    }
    public void saveAsTable(String tableNameArg) throws Exception{

    }

    public FlameRDD flatMap(PairToStringIterable lambda) throws Exception{
        byte[] serlambda=Serializer.objectToByteArray(lambda);
        String otname=context.invokeOperation(tablename, "/pair/flatMap", serlambda, null);
        FlameRDD answer= new FlameRDDImpl(otname, this.context);
        return answer;
    }

    public void destroy() throws Exception{

    }

    public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception{
        //same thing as flatmap lolz
        byte[] serlambda=Serializer.objectToByteArray(lambda);
        String otname=context.invokeOperation(tablename, "/pair/flatMapToPair", serlambda, null);
        FlamePairRDDImpl answer= new FlamePairRDDImpl(otname, this.context);
        return answer;
    }

	public FlamePairRDD join(FlamePairRDD other) throws Exception{
        String inputtable2=((FlamePairRDDImpl)other).tablename;
        String outputtablename=context.invokeJoinOperation(this.tablename, inputtable2, "/pair/join");
        FlamePairRDD answer= new FlamePairRDDImpl(outputtablename, this.context);

        return answer;
    }
    public FlamePairRDD cogroup(FlamePairRDD other) throws Exception{
        return null;
    }

}
