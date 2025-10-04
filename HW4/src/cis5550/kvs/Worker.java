package cis5550.kvs;
import cis5550.webserver.Server;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.util.Random;
//import cis5550.generic.Worker;


public class Worker extends cis5550.generic.Worker{
    public static void main(String[] args) {
        if (args.length!=3){
            System.err.println("wrong number of arguments in command line");
            System.exit(1);
        }
        int workerport=0;
        try {
            workerport=Integer.parseInt(args[0]); //remember to turn stirng to int
        } 
        catch ( NumberFormatException e) {
            System.err.println("the first argument(port) has to be a valid integer");
            System.exit(1);
        }
        String storagedir=args[1];
        String coordinatoraddy=args[2];
        Server.port(workerport);
        String workerid=null;
        Path idfilepath=Paths.get(storagedir, "id");
        File storagedirfile= new File(storagedir);
        try {
            if (!storagedirfile.exists()){
                if(!storagedirfile.mkdirs()){
                    System.err.println("failed to dcreate storage dir");
                    System.exit(1);
                }
            }
            if(Files.exists(idfilepath)){
                workerid=Files.readAllLines(idfilepath, StandardCharsets.UTF_8).get(0).trim();

                System.err.println("worker id read from file:"+workerid);
            }
            else{
                Random random = new  Random();
                String newid = ""; 
                for (int i=0; i<5; i++) {
                    char randomChar = (char)(random.nextInt(26) + 'a');
                    newid = newid+randomChar; 
                }
                workerid = newid;
                Files.write(idfilepath, workerid.getBytes(StandardCharsets.UTF_8));
                System.err.println("New Worker ID generated and written to file: " + workerid);
            } 
            if(workerid==null||workerid.isEmpty()){
                throw new IOException("worker id is empty");
            }
            
        } 
        catch (IOException e) {
            System.err.println("error accessing or writing worker id file");
            System.exit(1);
        }
        cis5550.generic.Worker.startPingThread(coordinatoraddy, storagedir, workerport, workerid);
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            System.err.println("Worker interrupted, shutting down");
}
    }
}
