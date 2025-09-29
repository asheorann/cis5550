//this is the package name
package cis5550.webserver;
//import cis5550.tools.Logger;
import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ServerSocketFactory;
import javax.net.ssl.*;

//main class (blueprint of the server), from which objects are called
public class Server {
    
    //Runs the Logger file
   // private static final Logger logger = Logger.getLogger(Server.class); //MY LOGGER, refer to the server.log file!
    private static Server instance = null;
    private static boolean running = false; //to see if the server thread is running
    private int port = 80; //basically this is the default if port is not called
    private String staticfilelocation = null; //configuration is stored

    private static void initializeServer(){
        if(instance == null){
            instance = new Server();
        }
    }
    public static void port(int p) {
        initializeServer();
        instance.port = p;
    }

    //okay i am going to make the secure stuff here
    private int securep = -1;
    public static void securePort(int p) {
        initializeServer();
        instance.securep = p;
    }
    private static void startServer(){
        initializeServer();
        if (running==false){
            running = true;
            Thread serverThread = new Thread(() -> instance.run());
            serverThread.start();
        }
    }
    //creating the routing list
    private List<RouteEntry> routes = new  ArrayList<>();
     Map<String, Session> sessions = new ConcurrentHashMap<>();
    public static void get(String path, Route route){
        startServer();
        instance.routes.add(new RouteEntry("GET", path, route));
    }
    public static void post(String path, Route route){
        startServer();
        instance.routes.add(new RouteEntry("POST", path, route));


    }
    public static void put(String path, Route route){
        startServer();
        instance.routes.add(new RouteEntry("PUT", path, route));

    }
    public static class staticFiles{
        public static void location(String s){
            startServer();
            instance.staticfilelocation = s;
        }
    }

    //METHOD: this is my error response, which sends a response back to a specific socket, with the following headers and message
    private static void showError(Socket sock, int code, String message) throws IOException{
        OutputStream out = sock.getOutputStream();
        String response = "HTTP/1.1 " + code + " " + message + "\r\n" + "Content-Type: text/plain\r\n" +"Connection: close\r\n" +"Server: cis5550.webserver.Server\r\n" +"Content-Length: " + message.length() + "\r\n" +"\r\n" + message;
        out.write(response.getBytes());
        out.flush();
    }
    //METHOD: guessing the content type for the file
    private static String guessContentType(File file) throws IOException {
        String type = Files.probeContentType(file.toPath()); //Here I get the type
        if (type == null) {
            return "application/octet-stream"; //found that this is typical for https
        }
        return type;
    }
    private static class RouteEntry {
        String method;
        Route handler;
        String path;

        public RouteEntry(String method, String path, Route handler){
            this.method=method;
            this.path=path;
            this.handler=handler;
        }
    }
    private void sLoop(ServerSocket listening_sock){
        // just copying and pasting from the while loop that i have in run
        while(true){
            try{
                Socket sock = listening_sock.accept();//I allowed that socket to accept incoming messages
            // logger.info("connection from:" +sock.getRemoteSocketAddress()); //I allowed that socket to accept incoming messages
                new Thread(() -> {
                    try {
                        actuallyServing((sock), this.staticfilelocation);
                    } catch (Exception e) {
                    //  logger.error("Error serving: " + e.getMessage());
                    } finally{
                        try{sock.close();} catch (IOException ignored) {}
                    }
                }).start();
            }
            catch (IOException e){
                System.out.println("error with the sLoop");
            }
        }
    }
    //THE MAIN METHOD WHERE THE CODE ENTERS!
    public void run() {
        //so first i just do my http port
        if(this.port!=-1){
            try {
                ServerSocket httpSock = new ServerSocket(this.port);
                new Thread(()-> sLoop(httpSock)).start();
            } catch (Exception e) {
                System.out.println("Throwing error because of not creating http port");
            }
        }

        if (this.securep!=-1){
            //this the line i m replacing ServerSocket listening_sock = new ServerSocket(this.port); //I created a socket with that socket number, that keeps listening for new ports
            try {
                String pwd = "secret";
                KeyStore keyStore = KeyStore.getInstance("JKS");
                keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());                
                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
                keyManagerFactory.init(keyStore, pwd.toCharArray());                
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(keyManagerFactory.getKeyManagers(), null, null);                
                ServerSocketFactory factory = sslContext.getServerSocketFactory();
                ServerSocket httpsSock = factory.createServerSocket(this.securep);  
                new Thread(()-> sLoop(httpsSock)).start();
            } catch (Exception e) {
                System.out.println("Error in starting secure http within run");
            }

        }
        
    }
    //gotta make the helper function
    private void parsequerypars(String qstring, Map<String, String> pars){
        if (qstring ==null){
            return;
        }
        String[] pairs = qstring.split("&") ;
        for (String pair : pairs) {
            String[] element = pair.split("=", 2);
            if (element.length == 2) {
                try {
                    String key = java.net.URLDecoder.decode(element[0], "UTF-8"); //prayin this works lol
                    String value = java.net.URLDecoder.decode(element[1], "UTF-8");
                    pars.put(key, value);
                } catch (Exception e) {
                    //wait i gotta COME BACK TO THIS IDK WHAT TO PUT HERE
                }
            }
        }
    }
    private boolean routematching(String routepath, String requrl, Map<String, String> pars){
        String[] rparts = routepath.split("/");
        String [] urlparts = requrl.split("/"); //splitting both these of based on /
        int rlen=rparts.length;
        if (urlparts.length!=rlen){
            return false;
        }

        for(int i =0; i<rlen;i+=1){
            String rpart = rparts[i];
            String urlpart= urlparts[i];
             if (rpart.startsWith(":")) {
                pars.put(rpart.substring(1), urlpart); //basically putting the parameter name and value and putting in map
            }
            else{
                if(!rpart.equals(urlpart)){
                    return false;
                }
            }
        }
        return true;
    }
    public void actuallyServing (Socket sock, String dir) throws IOException {
        InputStream in = sock.getInputStream();  //info is going from the socket to this in stream
        byte[] buf = new byte[8000]; //basically here create a buffer in which the incoming data/message can be read, 8000 indices in an empty array
        int full_read_len = 0; //will use below
        byte[] headerBytes = null;//i am temporarily setting headerbytes to null
        //Okay, this was previously just making the incoming data into a string, instead I need to keep it as bytes
        int headerEnd = -1;
        while(full_read_len<buf.length){ // could change this to the full read is less hten the buffer length
            int n = in.read(buf, full_read_len, buf.length-full_read_len); //in.read gives the number of bytes read in, and it can come in many chunks hence the loop
            if (n==-1) break; //when the client has finished sending in.read gives -1
            full_read_len+=n;
            //in the for loop below, I find out where the headers end    
            for (int i=3; i<full_read_len;i+=1){
                if(buf[i-3]==13 &&buf[i-2]==10&&buf[i-1]==13&&buf[i]==10){ //I copy the header piece in headerBytes
                    headerBytes = Arrays.copyOfRange(buf, 0, i+1);//I read the headerBytes and extrac the first line and the rest of the headers, the basic logic is it goes from bytes, to characters to per line
                    headerEnd = i+1;
                    // MAYBE CHANGE THIS HERE
                }
            }
            if (headerEnd!=-1){
                break;
            }
            if (headerBytes!=null){
                break;
            }
        }
        if (headerBytes ==null){
            sock.close();
           // logger.warn("No headers found");
            return;
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(headerBytes))); //headerbytes is a byte array [71, 60 ..], which is converted into ['G'], 'E', then the buffered reader reads each line at a tiem
        String requestLine = reader.readLine(); //the first request line
            
        if (requestLine==null || requestLine.isEmpty()){
            sock.close();
            return;
        }
        // logger.info("Request line: "+requestLine); //logs the request line each time

        //here I am just splitting up the request line
        String[] request_line_parts = requestLine.split(" ");
        if (request_line_parts.length!=3){
            showError(sock, 400, "Bad Request");
            return;
        }
        String method=request_line_parts[0];
        String url=request_line_parts[1];
        String version = request_line_parts[2];
        //no reading all the headers into a map
        Map<String, String> headers= new HashMap<>();
        Map<String, String> querypars = new HashMap<>(); // making another map for the last step 
        //okay now adding a check for the ? in the url 
        if (url.contains("?")){
            int indexofquestion = url.indexOf('?');
            String qstring = url.substring(indexofquestion+1);
            url = url.substring(0, indexofquestion);
            //now imma call the helper function i made earlier
            parsequerypars(qstring, querypars);
        }
        

        String line;
        while((line=reader.readLine())!=null && !line.isEmpty()){
            String [] headerComps = line.split(":", 2);
            if (headerComps.length==2){
                headers.put(headerComps[0].trim().toLowerCase(), headerComps[1].trim()); //here i am putting the headers into the hashmap
            }
        }
        //Now, imma read the body
        byte[] bodyBits = new byte[0];
        if (headers.containsKey("content-length")){
            int contentLength = Integer.parseInt(headers.get("content-length")); //get the content length
            bodyBits = new byte[contentLength]; //create the array of that length
            int alreadyRead = full_read_len-headerEnd;
            System.arraycopy(buf, headerEnd, bodyBits, 0, Math.min(alreadyRead, contentLength));
            int totalRead = alreadyRead;
            while (totalRead < contentLength) {
                int n = in.read(bodyBits, totalRead, contentLength - totalRead);
                if (n == -1) break;
                totalRead += n;
            }
           // sock.getInputStream().read(bodyBits, 0, contentLength);
        }
        //checking the content type rq
        String contenttype= headers.get("content-type");
        if (contenttype!=null && contenttype.startsWith("application/x-www-form-urlencoded")) {
            try {
                String bodys = new String(bodyBits, "UTF-8");
                parsequerypars(bodys, querypars);
            } catch (Exception e){
                //again ig im jsut ignoring not sure what to do here come back
            } 
        }

        // now time to figure out the routing loigc

        Boolean foundroute = false;
        for (RouteEntry route : routes) { //so i am accessing my routing table
            Map<String, String> pathpars = new HashMap<>();
            if (route.method.equals(method) && routematching(route.path, url, pathpars)){
                foundroute = true;
                try {
                    Response res = new ResponseImpl(sock.getOutputStream()); 
                    Request req= new RequestImpl(method, url, version, headers, querypars, pathpars, (InetSocketAddress)sock.getRemoteSocketAddress(), bodyBits, this, res);
                     
                    Object result = route.handler.handle(req, res);
                    //building the response to send back
                    ResponseImpl resImpl = (ResponseImpl) res;
                    if (result!=null){
                        resImpl.body(result.toString());
                    }        
                    if (resImpl.getBody()!=null){
                        resImpl.header("content-length", String.valueOf(resImpl.getBody().length)); //just adding the length just if the user did not
                    }
                    //now imma send the status line, heades and body to the client
                    if (!resImpl.writtenheaders()){
                        OutputStream out = sock.getOutputStream();
                        String statusLine = "HTTP/1.1 "+resImpl.getStatusCode() +" "+resImpl.getStatusText()+"\r\n";
                        out.write(statusLine.getBytes());

                        //here looping thru all the headers in response object and writing them
                        for(Map.Entry<String, String> entry : resImpl.getHeaders().entrySet()){
                            out.write((entry.getKey() +": "+entry.getValue() + "\r\n").getBytes());
                        }
                        out.write("\r\n".getBytes()); //to differentiate headers from the body
                        if (resImpl.getBody()!=null){
                            out.write(resImpl.getBody());
                        }
                        out.flush();
                    }
                    
                } catch ( Exception e) {
                    showError(sock, 500, "Internal Server Error");
                }
                break;
            }
        }
        if (foundroute==false){
            if(dir!=null){
                File file = new File(dir, url); //finds the file
                if (!file.exists()){
                    showError(sock, 404, "Not Found");
                    return;
                }
                if (!file.canRead()) {
                    showError(sock, 403, "Forbidden");
                    return;
                }
                else{
                    OutputStream outputstream = sock.getOutputStream();
                    String headerstext = "HTTP/1.1 200 OK\r\n" + "Content-Type: " + guessContentType(file) + "\r\n" +"Server: cis5550.webserver.Server\r\n" +"Content-Length: " + file.length() + "\r\n" +"\r\n"; // two indents at the end
                    outputstream.write(headerstext.getBytes()); //this is me sending the headers to the client
                    //now I send the actual file message
                    if (method.equals("GET")){
                        FileInputStream filestream = new FileInputStream(file);
                        byte[] buf2 = new byte [8000];
                        int a;
                        while ((a=filestream.read(buf2))!=-1){         //same logic I been using, write it into the buffer until we reach the end
                            outputstream.write(buf2,0,a);
                        }
                        filestream.close();       
                    } 
                    outputstream.flush();
  
                }
            }
            else{
                showError(sock, 404, "Not Found");
            }
           // sock.close();
        }
        sock.close();
        //since I have the request line, I can break that apart and check for a few errors:
    }
    
}
