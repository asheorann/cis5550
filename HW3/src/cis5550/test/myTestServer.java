package cis5550.test;
import static cis5550.webserver.Server.*;

public class myTestServer {
    public static void main (String[] args){
        securePort(443);
        get("/", (request, response) ->{
            return "Hello World - this is Anushka Sheoran";
        });
        System.out.println("my test server starting on https port 8443");
    }
}

