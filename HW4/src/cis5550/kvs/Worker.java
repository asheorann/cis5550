package cis5550.kvs;
import cis5550.webserver.Server;
//import cis5550.generic.Worker;


public class Worker extends cis5550.generic.Worker{
    public static void main(String[] args) {
        if (args.length!=3){
            System.out.println("wrong number of arguments in command line");
        }
        int workerport=0;
        try {
            workerport=Integer.parseInt(args[0]); //remember to turn stirng to int
        } 
        catch ( NumberFormatException e) {
            System.out.println("the first argument(port) has to be a valid integer");
            System.exit(1);
        }
        String storagedir=args[1];
        String coordinatoraddy=args[2];
        Server.port(workerport);
        cis5550.generic.Worker.startPingThread(coordinatoraddy, storagedir, workerport);
    }
}
