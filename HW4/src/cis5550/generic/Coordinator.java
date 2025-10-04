package cis5550.generic;
import java.util.*;
import cis5550.webserver.Server;
import cis5550.webserver.Response;
import cis5550.webserver.Request;
import cis5550.kvs.WorkerEntry;

public class Coordinator {
    //for right now just retruning an empty lsit
    private static final long workertimeoutTIME=15000;

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
        List<WorkerEntry> activeworkers= sortedWorkers();
        String wTable="<h2>Active Workers (real)</h2>"+ "<table border=\"1\"><tr><th>ID</th><th>IP:Port</th><th>Link</th></tr>";
        if(activeworkers.isEmpty()){
            wTable+="<tr><td colspan=\"2\">No active workers as of now.</td></tr>";
        }
        else{
            for(WorkerEntry worker: activeworkers){
                String ipport=worker.ip+":"+worker.port;
                String url="http://"+ipport+"/";
                wTable+="<tr>";
                wTable+="<td>" + worker.id + "</td>";
                wTable+= "<td>"+"<a href=\"" + url + "\">" + ipport + "</a>"+"</td>";
                wTable+= "</tr>";
            }
        }
        wTable+="</table>"; //ended the table lolz
        System.err.println("workerTable() called: " + activeworkers.size() + " active workers");
        for (WorkerEntry w : activeworkers) {
            System.err.println("  -> " + w.id + " at " + w.ip + ":" + w.port + " (lastPing=" + w.lastPingTime + ")");
        }

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
            long currenttime=System.currentTimeMillis();
            if (id==null||port == null) {
                res.status(400, "Bad Request");
                res.body("ID and port parameters are neeeded");
                return null;
            }
            String ip = req.ip(); //this is the ip address of the worker
            cis5550.kvs.Coordinator.activeWorkers.compute(id, (key, worker) -> { 
                if (worker==null) {
                    worker=new WorkerEntry(id, ip, port); //so this the 
                    worker.lastPingTime=currenttime;
                    System.err.println("Registere worker: " +id + " at " +ip+":"+port);
                } 
                else {
                    worker.ip =ip;
                    worker.port =port;
                    worker.lastPingTime =currenttime;
                }

                return worker;
            });
            res.status(200, "OK");
            res.body("OK"); //yay it worked
            System.err.println("PING RECEIVED: id=" + id + " ip=" + ip + " port=" + port);
            System.err.println("Map size after update = " + cis5550.kvs.Coordinator.activeWorkers.size());

            return null;
        });
    }
    private static List<WorkerEntry> sortedWorkers(){
        List<WorkerEntry> allWorkers=new java.util.ArrayList<>(cis5550.kvs.Coordinator.activeWorkers.values());
        long currenttime=System.currentTimeMillis();
        List<WorkerEntry> activeworkers = new ArrayList<>();
        for (WorkerEntry worker: allWorkers){
            long timesincelastping=currenttime-worker.lastPingTime;
            if(timesincelastping<=workertimeoutTIME){
                activeworkers.add(worker);
            }
        }
        Collections.sort(activeworkers, (w1, w2) ->  w1.id.compareTo(w2.id)); //basicallu just sorting the list by id
        return activeworkers;
        
    }
}
