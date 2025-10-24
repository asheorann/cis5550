package cis5550.flame;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.Vector;

import cis5550.kvs.KVSClient;
import cis5550.tools.Logger;
import cis5550.tools.Partitioner;
import cis5550.tools.Partitioner.Partition;
import cis5550.tools.HTTP;

import java.io.IOException;
import java.net.URLEncoder;

public class FlameContextImpl implements FlameContext{
    StringBuilder outputbuf;
    KVSClient kvs; //this gonna hold froom the coordinatosr
    //i shoudl add a counter for ht eunique table names
    static int tablecount=0;
    Vector<String> flameworkers; //list of the ip port, using this instead of normal lists because this is better for multithreaded 
    String kvsmainaddress; // this is the ip:port of kvs coordinatoir

    public FlameContextImpl(String jarname, StringBuilder outputbuf, KVSClient kvs, Vector<String> flameworkers, String kvsmainaddress){
        this.outputbuf=outputbuf;
        this.kvs=kvs;
        this.flameworkers=flameworkers;
        this.kvsmainaddress=kvsmainaddress;
    }
    public KVSClient getKVS(){
        return this.kvs;
    }
    public void output(String s){
        this.outputbuf.append(s);
    }

    // This function should return a FlameRDD that contains the strings in the provided
    // List. It is okay for this method to run directly on the coordinator; it does not
    // need to be parallelized.

    public FlameRDD parallelize(List<String> list) throws Exception{
        long curtime=System.currentTimeMillis();
        String tablename="rdd-"+ curtime+"-"+tablecount;
        //print(tablename)
        tablecount+=1;
        for (String item: list){
            String rowkey=UUID.randomUUID().toString();
            kvs.put(tablename,rowkey,"value", item);
        }
        FlameRDDImpl newone= new FlameRDDImpl(tablename, this);
        return newone;
    }
    String invokeOperation (String inputtable, String oppath, byte[] serlambda, String zelement) throws Exception{
        String outputtable="rdd-"+System.currentTimeMillis()+"-"+tablecount;
        tablecount+=1;
        Partitioner part= new Partitioner(); //this is to help calculat ehte asissgments
        Vector<String> kvsworkers= new Vector<>(); //getting the list
        int numworkers=kvs.numWorkers();
        for(int i=0;i<numworkers;i+=1){
            String wid=kvs.getWorkerID(i);
            String waddy=kvs.getWorkerAddress(i);
            if(wid!=null && waddy!=null){
                kvsworkers.add(wid+","+waddy); //i thought there was a method we could get the list from but since i dont see it gotta make it ourselves
            }
        }
        if(kvsworkers.isEmpty()){
            throw new IOException("failed to get kvs worker info");
        }
        //print(kvsworkers)
         //jave to sort by their ids to defint he key range
        Collections.sort(kvsworkers);
        int lim=kvsworkers.size();
        for (int i=0; i<lim;i+=1){
            String kvsworkerinfo=  kvsworkers.get(i); 
            String parts[]=kvsworkerinfo.split(",");
            String kvsworkerid=parts[0]; //unique id
            String kvsworkeraddy=parts[1];
            String nextid=null;
            if(i<lim-1){
                nextid=kvsworkers.get(i+1).split(",")[0]; //basically if its th elast one then next one is null
                 //print(nextid)
            }
            part.addKVSWorker(kvsworkeraddy, kvsworkerid, nextid); //gotta add the worker to the partioner
        }


        if (lim>0){
            String lastaddy=kvsworkers.lastElement().split(",")[1]; //this last worker is also responsible fo rht ekes that are smaller then the first worker id, according to the hwwwwwwwww ugh i wanna gts
            String firstid=kvsworkers.firstElement().split(",")[0];
            part.addKVSWorker(lastaddy, null, firstid);
        }
        for(String flameaddy: this.flameworkers){
            part.addFlameWorker(flameaddy); //basically telling the partioner abot the avalable flame workers
        }
        //now gota calculate the opimtal assingments
        Vector<Partition> parts=part.assignPartitions();
        //gontta make one vector to keep trakc o the worker threads and one of to record if the task suceeded or failed
        Vector<Thread> workerthread=new Vector<>();
        Vector<Boolean> tasksuccess=new Vector<>(); 
        for (Partition p: parts){
            Thread t=new Thread(()->{
                try {
                    String assignedworker=p.assignedFlameWorker;
                    String startkeyrange=p.fromKey;
                    String endkeyrange=p.toKeyExclusive;
                    String workerurl="http://"+assignedworker+oppath+"?in="+inputtable+"&out="+outputtable+"&kvs="+this.kvsmainaddress;
                    if(startkeyrange!=null){
                        workerurl+="&from="+URLEncoder.encode(startkeyrange,"UTF-8");
                    }
                    if(endkeyrange!=null){
                        workerurl+="&to="+URLEncoder.encode(endkeyrange,"UTF-8");
                    }
                    if(zelement!=null){
                        workerurl +="&zero="+URLEncoder.encode(zelement,"UTF-8");
                    }
                    HTTP.Response workerresponse=HTTP.doRequest("POST",workerurl,serlambda); //sending it in the request body
                    if (workerresponse.statusCode()!=200||!new String(workerresponse.body()).equals("OK")){
                        throw new Exception("worker at " +assignedworker+"failed");
                    }
                    synchronized (tasksuccess) {
                        tasksuccess.add(true);
                    }
                } catch (Exception e) {
                    synchronized (tasksuccess) {
                        tasksuccess.add(false);
                    }
                }
            });
            //now gonna add the thread i just made to the list and star tit
            workerthread.add(t);
            t.start();
        }
        for (Thread t: workerthread){
            t.join();
             //print(t)
        }
        if(tasksuccess.size()!=parts.size()||tasksuccess.contains(false)){ //this is me checking if any worer faileddd
            throw new RuntimeException("operation failed");
        }
        return outputtable;

    }

}
