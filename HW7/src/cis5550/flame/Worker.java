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

  static String jarname;
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
    //from table post
    post("/context/fromTable", (request, response) ->{
      if(!myJAR.exists()){
        response.status(500, "Internal Server Error");
        return "no jar loaded";
      }
      try{
        String inputtable=request.queryParams("in");
        String outputtable=request.queryParams("out");
        String kvscoord=request.queryParams("kvs");
        String fromkey=request.queryParams("from");
        String tokey=request.queryParams("to"); //this could be null and so could th eform key

        byte[] lambabytes=request.bodyAsBytes();
        FlameContext.RowToString lambda=(FlameContext.RowToString) Serializer.byteArrayToObject(lambabytes, myJAR);
        KVSClient kvs= new KVSClient(kvscoord);

        //now gonna scan the rangeo f the input table
        Iterator<Row> rows=kvs.scan(inputtable, fromkey, tokey);
        while(rows.hasNext()){
          Row row=rows.next();
          String result=lambda.op(row); //gonna run the lambda on the row
          if(result!=null){
            String newrowkey=UUID.randomUUID().toString();
            kvs.put(outputtable, newrowkey, "value", result);
          }
        }
        //response.body("OK");
        return "OK";
      } catch (Exception e){
          e.printStackTrace();
          response.status(500, "Internal Server Error");
          return "ERRORRR"+e.getMessage(); //come back her and put the proper error bruh
      }

    }); 

    //from flat map to pair post
    post("/rdd/flatMapToPair", (request, response) ->{
      try{
        String inputtable=request.queryParams("in");
        String outputtable=request.queryParams("out");
        String kvscoord=request.queryParams("kvs");
        String fromkey=request.queryParams("from");
        String tokey=request.queryParams("to"); //this could be null and so could th eform key

        byte[] lambabytes=request.bodyAsBytes();
        KVSClient kvs= new KVSClient(kvscoord);
        FlameRDD.StringToPairIterable lambd=(FlameRDD.StringToPairIterable) Serializer.byteArrayToObject(lambabytes,myJAR);


        Iterator<Row> rows=kvs.scan(inputtable, fromkey, tokey); //literally same as the functionabove
        while(rows.hasNext()){
          Row row=rows.next();
          String val=row.get("value"); 
          if(val!=null){
            Iterable<FlamePair> pairs=lambd.op(val);
            if(pairs!=null){
              for(FlamePair pair: pairs){
                String uniqueColName=UUID.randomUUID().toString();
                kvs.put(outputtable, pair._1(), uniqueColName, pair._2());
              }
            }
          }
        }
        //response.body("OK");
        return "OK";
      } catch (Exception e){
          e.printStackTrace();
          response.status(500, "Internal Server Error");
          return "ERRORRR"+e.getMessage(); //come back her and put the proper error bruh
      }
    });
    //FLAT MAP
    post("/pair/flatMap", (request, response) ->{
      try{
        String inputtable=request.queryParams("in");
        String outputtable=request.queryParams("out");
        String kvscoord=request.queryParams("kvs");
        String fromkey=request.queryParams("from");
        String tokey=request.queryParams("to"); //this could be null and so could th eform key

        byte[] lambabytes=request.bodyAsBytes();
        KVSClient kvs= new KVSClient(kvscoord);
        FlamePairRDD.PairToStringIterable lambd=(FlamePairRDD.PairToStringIterable) Serializer.byteArrayToObject(lambabytes,myJAR);


        Iterator<Row> rows=kvs.scan(inputtable, fromkey, tokey); //literally same as the functionabove
        while(rows.hasNext()){
          Row row=rows.next();
          String key=row.key();
          for (String col: row.columns()){
            String val=row.get(col); 
            FlamePair inputpair=new FlamePair(key, val);
            Iterable<String> results=lambd.op(inputpair);
            if(results!=null){
              for(String s: results){
                String uniquekey=UUID.randomUUID().toString();
                kvs.put(outputtable, uniquekey, "value", s);
              }
            }
          }
        }
        //response.body("OK");
        return "OK";
      } catch (Exception e){
          e.printStackTrace();
          response.status(500, "Internal Server Error");
          return "ERRORRR"+e.getMessage(); //come back her and put the proper error bruh
      }
    });

    //FLAT MAPTOPAIR
    post("/pair/flatMapToPair", (request, response) ->{
      try{
        String inputtable=request.queryParams("in");
        String outputtable=request.queryParams("out");
        String kvscoord=request.queryParams("kvs");
        String fromkey=request.queryParams("from");
        String tokey=request.queryParams("to"); //this could be null and so could th eform key

        byte[] lambabytes=request.bodyAsBytes();
        KVSClient kvs= new KVSClient(kvscoord);
        FlamePairRDD.PairToPairIterable lambd=(FlamePairRDD.PairToPairIterable) Serializer.byteArrayToObject(lambabytes,myJAR);


        Iterator<Row> rows=kvs.scan(inputtable, fromkey, tokey); //literally same as the functionabove
        while(rows.hasNext()){
          Row row=rows.next();
          String key=row.key();
          for (String col: row.columns()){
            String val=row.get(col); 
            FlamePair inputpair=new FlamePair(key, val);
            Iterable<FlamePair> results=lambd.op(inputpair);
            if(results!=null){
              for(FlamePair s: results){
                String uniquekey=UUID.randomUUID().toString();
                kvs.put(outputtable, s._1(), uniquekey, s._2());
              }
            }
          }
        }
        //response.body("OK");
        return "OK";
      } catch (Exception e){
          e.printStackTrace();
          response.status(500, "Internal Server Error");
          return "ERRORRR"+e.getMessage(); //come back her and put the proper error bruh
      }
    });
    //DISTINCT
    post("/rdd/distinct", (request, response) ->{
      try{
        String inputtable=request.queryParams("in");
        String outputtable=request.queryParams("out");
        String kvscoord=request.queryParams("kvs");
        String fromkey=request.queryParams("from");
        String tokey=request.queryParams("to"); //this could be null and so could th eform key

        KVSClient kvs= new KVSClient(kvscoord);
        Iterator<Row> rows=kvs.scan(inputtable, fromkey, tokey); //literally same as the functionabove
        while(rows.hasNext()){
          Row row=rows.next();
          String value=row.get("value");
            if(value!=null){
                kvs.put(outputtable, value, "value", value);
              }
        }
        //response.body("OK");
        return "OK";
      } catch (Exception e){
          e.printStackTrace();
          response.status(500, "Internal Server Error");
          return "ERRORRR"+e.getMessage(); //come back her and put the proper error bruh
      }
    });


  //JOINNN
    post("/pair/join", (request, response) ->{
      try{
        String inputtable1=request.queryParams("in1");
        String inputtable2=request.queryParams("in2");

        String outputtable=request.queryParams("out");
        String kvscoord=request.queryParams("kvs");
        String fromkey=request.queryParams("from");
        String tokey=request.queryParams("to"); //this could be null and so could th eform key

        KVSClient kvs= new KVSClient(kvscoord);
        Iterator<Row> rows=kvs.scan(inputtable1, fromkey, tokey); //literally same as the functionabove
        while(rows.hasNext()){
          Row row=rows.next();
          String key=row.key();
          Row row2=kvs.getRow(inputtable2, key); //looking up the same key in the second table
          if(row2!=null){
            for(String col1: row.columns()){ //basicallly i fh the key exists in both the columns then gonna join them, nested loop to make lla the combinations
              String val1=row.get(col1);
              for(String col2: row2.columns()){
                String val2=row2.get(col2);

                String comb=val1+","+val2;
                String outname=Hasher.hash(col1+":"+col2); //usng hasher form tools to make a unique colum nanme
                kvs.put(outputtable, key, outname, comb);
              }
          }
         }
        }
        //response.body("OK");
        return "OK";
      } catch (Exception e){
          e.printStackTrace();
          response.status(500, "Internal Server Error");
          return "ERRORRR"+e.getMessage(); //come back her and put the proper error bruh
      }
    });
    //fOLDDD LESS GO LAST ONE!
    post("/rdd/fold", (request, response) ->{
      try{
        String inputtable=request.queryParams("in");
        String outputtable=request.queryParams("out");
        String kvscoord=request.queryParams("kvs");
        String fromkey=request.queryParams("from");
        String tokey=request.queryParams("to"); //this could be null and so could th eform key
        String zeroelement=URLDecoder.decode(request.queryParams("zero"), "UTF-8");

        byte[] lambabytes=request.bodyAsBytes();
        KVSClient kvs= new KVSClient(kvscoord);
        FlamePairRDD.TwoStringsToString lambd=(FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(lambabytes,myJAR);
        Iterator<Row> rows=kvs.scan(inputtable, fromkey, tokey); //literally same as the functionabove

        String loc=zeroelement; //imma init a local accumatlator
        while(rows.hasNext()){
          Row row=rows.next();
          String value=row.get("value");
            if(value!=null){
              loc=lambd.op(loc, value);
            }
        }
        //response.body("OK");
        return loc;
      } catch (Exception e){
          e.printStackTrace();
          response.status(500, "Internal Server Error");
          return "ERRORRR"+e.getMessage(); //come back her and put the proper error bruh
      }
    });    

	}
}
