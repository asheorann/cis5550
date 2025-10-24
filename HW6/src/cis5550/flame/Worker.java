package cis5550.flame;

import cis5550.flame.FlamePairRDD.TwoStringsToString;
import java.util.*;
import java.net.*;
import java.io.*;
import cis5550.flame.FlameRDD.StringToPair;
import static cis5550.webserver.Server.*;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import cis5550.kvs.*;
import cis5550.webserver.Request;

class Worker extends cis5550.generic.Worker {

	public static void main(String args[]) {
    if (args.length != 2) {
    	System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
    	System.exit(1);
    }

    int port = Integer.parseInt(args[0]);
    String server = args[1];
	  startPingThread(server, ""+port, port);
    final File myJAR = new File("__worker"+port+"-current.jar");

  	port(port);

    post("/useJAR", (request,response) -> {
      FileOutputStream fos = new FileOutputStream(myJAR);
      fos.write(request.bodyAsBytes());
      fos.close();
      return "OK";
    });

    //the flatmpa function
    post("/rdd/flatMap", (request, response)->{
      if(!myJAR.exists()){
        response.status(500, "Internal Server Error");
        return "no jar loaded";
      }
      String inputtable=request.queryParams("in");
      String outputtable=request.queryParams("out");
      String kvscoord=request.queryParams("kvs");
      String fromkey=request.queryParams("from");
      String tokey=request.queryParams("to"); //this could be null and so could th eform key
      if(inputtable==null||outputtable==null||kvscoord==null){
        response.status(400, "Bad Request");
        return "missing parameters";
      }
      try {
          byte[] lbytes=request.bodyAsBytes();
          //String jarpath=myJAR.getAbsolutePath(); damn did not need to write this
          FlameRDD.StringToIterable lambda= (FlameRDD.StringToIterable) Serializer.byteArrayToObject(lbytes, myJAR); //need to cast it cuz needa to tell it that i know the object is accc a stringtoterable
          KVSClient kvs=new KVSClient(kvscoord);
          Iterator<Row> rows=kvs.scan(inputtable, fromkey, tokey);
          //now gotta process each of the rows
          //omg i WANNA SLEEEPEPPEPEPEPPEPE
          while(rows.hasNext()){
            Row row=rows.next();
            String value=row.get("value"); //the RDD store data in the value column
            if(value!=null){
              Iterable<String> results=lambda.op(value);
              //basically if lambda gave results then put them in the output table
              if(results!=null){
                for(String rstring: results){
                  String newkey= UUID.randomUUID().toString(); //make new random key for each output element
                  kvs.put(outputtable, newkey, "value", rstring);

                }
              }
            }

          }
          return "OK";
      } catch (Exception e) {
         response.status(500, "Internal Server Error");
         return "ERRORRRR"; //come back and write the proper catching of exceptionsss

      }

    });
    //maptopair method
    post("/rdd/mapToPair", (request, response)->{
      if(!myJAR.exists()){
        response.status(500, "Internal Server Error");
        return "no jar loaded";
      }
      String inputtable=request.queryParams("in");
      String outputtable=request.queryParams("out");
      String kvscoord=request.queryParams("kvs");
      String fromkey=request.queryParams("from");
      String tokey=request.queryParams("to"); //this could be null and so could th eform key
      if(inputtable==null||outputtable==null||kvscoord==null){
        response.status(400, "Bad Request");
        return "missing parameters";
      }
      try {
          byte[] lbytes=request.bodyAsBytes();
          //String jarpath=myJAR.getAbsolutePath(); damn did not need to write this
          FlameRDD.StringToPair lambda= (StringToPair) Serializer.byteArrayToObject(lbytes, myJAR); //need to cast it cuz needa to tell it that i know the object is accc a stringtoterable
          KVSClient kvs=new KVSClient(kvscoord);
          Iterator<Row> rows=kvs.scan(inputtable, fromkey, tokey);
          //now gotta process each of the rows
          //omg i WANNA SLEEEPEPPEPEPEPPEPE
          while(rows.hasNext()){
            Row row=rows.next();
            String ogkey=row.key();
            String value=row.get("value"); //the RDD store data in the value column
            if(value!=null){
              FlamePair pair=lambda.op(value);
              //basically if lambda gave results then put them in the output table
              if(pair!=null){
                  kvs.put(outputtable, pair._1(), ogkey, pair._2()); //got this from acc looking at the file defs lol
              }
            }

          }
          return "OK";
      } catch (Exception e) {
         response.status(500, "Internal Server Error");
         return "ERRORRRR"; //come back and write the proper catching of exceptionsss

      }

    });

    //FOLD BY KEY method
    post("/pair/foldByKey", (request, response)->{
      if(!myJAR.exists()){
        response.status(500, "Internal Server Error");
        return "no jar loaded";
      }
      String inputtable=request.queryParams("in");
      String outputtable=request.queryParams("out");
      String kvscoord=request.queryParams("kvs");
      String fromkey=request.queryParams("from");
      String tokey=request.queryParams("to"); //this could be null and so could th eform key
      String zeroelementenc=request.queryParams("zero");
      if(inputtable==null||outputtable==null||kvscoord==null){
        response.status(400, "Bad Request");
        return "missing parameters";
      }
      String zeroelement=URLDecoder.decode(zeroelementenc, "UTF-8");
      try {
          byte[] lbytes=request.bodyAsBytes();
          //String jarpath=myJAR.getAbsolutePath(); damn did not need to write this
          TwoStringsToString lambda= (TwoStringsToString) Serializer.byteArrayToObject(lbytes, myJAR); //need to cast it cuz needa to tell it that i know the object is accc a stringtoterable
          KVSClient kvs=new KVSClient(kvscoord);
          Iterator<Row> rows=kvs.scan(inputtable, fromkey, tokey);
          //now gotta process each of the rows
          //omg i WANNA SLEEEPEPPEPEPEPPEPE
          while(rows.hasNext()){ //basically propercessing each row, each row has a unique key k
            Row row=rows.next();
            String k=row.key();
            String acc=zeroelement; //initalize the acc with the zero element
            for(String c: row.columns()){ //go through all columns values v fo rhtis key k
              String v=row.get(c);
              if (v!=null){
                acc=lambda.op(acc, v); //apply the lambda
              }
            }
            kvs.put(outputtable, k, "value", acc); //got this from acc looking at the file defs lo
          }
          return "OK";
      } catch (Exception e) {
         response.status(500, "Internal Server Error");
         return "ERRORRRR"; //come back and write the proper catching of exceptionsss

      }

    });
	}
}
