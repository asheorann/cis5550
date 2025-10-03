package cis5550.generic;
import java.util.*;
import cis5550.webserver.Server;
import cis5550.webserver.Response;
import cis5550.webserver.Request;
import java.util.Collections;
import java.util.Map;
import cis5550.kvs.WorkerEntry;

public class Coordinator {
    //for right now just retruning an empty lsit
    public static List<String> getWorkers(){
        List<WorkerEntry> sortedworkers = sortedWorkers();
        List<String> workeraddresses = new ArrayList<>();
        for (WorkerEntry worker: sortedworkers){
            String addy=worker.ip+":"+worker.port;
            workeraddresses.add(addy);
        }
        return workeraddresses;
    }
    public static String workerTable(){
        String wTable="<h2>Active Workers (dumdum)</h2>"
             + "<table border=\"1\"><tr><th>ID</th><th>IP:Port</th><th>Link</th></tr>"
             + "<tr><td colspan=\"3\">No workers as of nowww.</td></tr>"
             + "</table>";
    
        return wTable;
    }
    public static void registerRoutes(){
        Server.get("/workers", (Request req, Response res) ->{
            List<WorkerEntry> sortedworkers=sortedWorkers();
            int k=sortedworkers.size();
            String result=k+"\n";
            for(WorkerEntry worker: sortedworkers){
                result=result+worker.id + ","+worker.ip+":"+worker.port+"\n";
            }
            res.type("text/plain");
            res.body(result);
            return null;
        });
        Server.get("/ping",(Request req, Response res) -> {
            String id= req.queryParams("id");
            String port= req.queryParams("port");
            if (id==null||port == null) {
                res.status(400, "Bad Request");
                res.body("ID and port parameters are neeeded");
                return null;
            }
            String ip = req.ip(); //this is the ip address of the worker
            cis5550.kvs.Coordinator.activeWorkers.compute(id, (key, worker) -> { 
                if (worker==null) {
                    worker=new WorkerEntry(id, ip, port); //so this the 
                    System.out.println("Registere worker: " +id + " at " +ip+":"+port);
                } 
                else {
                    worker.ip =ip;
                    worker.port =port;
                    worker.lastPingTime =System.currentTimeMillis();
                }
                return worker;
            });
            res.status(200, "OK");
            res.body("OK"); //yay it worked
            return null;
        });
    }
    private static List<WorkerEntry> sortedWorkers(){
        List<WorkerEntry> allWorkers=new java.util.ArrayList<>(cis5550.kvs.Coordinator.activeWorkers.values());
        Collections.sort(allWorkers, (w1, w2) ->  w1.id.compareTo(w2.id)); 
        return allWorkers;
        
    }
}
