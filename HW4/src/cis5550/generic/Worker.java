package cis5550.generic;
import cis5550.webserver.Server;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.net.HttpURLConnection;
import java.net.URL;

public class Worker {
    public static void startPingThread(String coordinatoraddy, String storagedir, int workerport, String workerid){
        String finalworkerid=workerid;
        String portstring=String.valueOf(workerport);
        Thread pingthread=new Thread(()->{
            while(!Thread.currentThread().isInterrupted()){
                try {
                    String pingurl="http://"+coordinatoraddy+"/ping?id="+finalworkerid+"&port="+portstring;
                    URL url=new URL(pingurl);
                    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                    conn.setConnectTimeout(2000); 
                    conn.setReadTimeout(2000);
                    conn.setRequestMethod("GET");
                    conn.connect();
                    int responseCode=conn.getResponseCode();
                    if (responseCode!= 200) {
                        System.err.println("Ping failled with this code: " + responseCode+ " for " +pingurl);
                    } 
                    else{
                        System.err.println("Ping successful to " + pingurl);
                    }
                    conn.disconnect();
                } 
                catch ( Exception e) {
                    System.err.println("ping to coordinator failed"+e.getMessage());
                }
                try {
                    Thread.sleep(5000); // milliseconds to second
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "Worker-ping-thread");
        //pingthread.setDaemon(true); //im actually begging please work and autograder please please work
        pingthread.start();

    }
}
