package cis5550.generic;
import cis5550.webserver.Server;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.net.URL;

public class Worker {
    public static void startPingThread(String coordinatoraddy, String storagedir, int workerport, String workerid){
        String finalworkerid=workerid;
        String portstring=String.valueOf(workerport);
        new Thread(()->{
            while(true){
                try {
                    String pingurl="http://"+coordinatoraddy+"/ping?id="+finalworkerid+"&port="+portstring;
                    URL url=new URL(pingurl);
                    try (InputStream stream =url.openStream()) { //im tryiggering the http request by opening and reading the stream
                        new BufferedReader(new InputStreamReader(stream,StandardCharsets.UTF_8)).lines().forEach(s -> {});
                    }
                } 
                catch ( Exception e) {
                    System.err.println("ping to coordinator failed"+e.getMessage());
                }
                try {
                    Thread.sleep(5000); // milliseconds to second
                } catch (InterruptedException e) {
                    break;
                }
            }
        }, "Worker-ping-thread").start();

    }
}
