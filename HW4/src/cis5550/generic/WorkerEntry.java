package cis5550.generic;
//earlier was in the coordinator file not sure if that is okay, need to make it public so that its accessible so putting in enw file
public class WorkerEntry {
    public String id;
    public String ip;
    public String port;
    public long lastPingTime;

    public WorkerEntry(String id, String ip, String port){
        this.id=id;
        this.ip=ip;
        this.port=port;
        //this.lastPingTime=System.currentTimeMillis();
    }

}
