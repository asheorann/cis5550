//this is the package name
package cis5550.webserver;
import cis5550.tools.Logger;
import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.util.Arrays;

//main class (blueprint of the server), from which objects are called
public class Server {
    
    //Runs the Logger file
    private static final Logger logger = Logger.getLogger(Server.class); //MY LOGGER, refer to the server.log file!

    //METHOD: this is my error response, which sends a response back to a specific socket, with the following headers and message
    private static void showError(Socket sock, int code, String message) throws IOException{
        OutputStream out = sock.getOutputStream();
        String response = "HTTP/1.1 " + code + " " + message + "\r\n" + "Content-Type: text/plain\r\n" +"Server: cis5550.webserver.Server\r\n" +"Content-Length: " + message.length() + "\r\n" +"\r\n" + message;
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

    //THE MAIN METHOD WHERE THE CODE ENTERS!
    public static void main(String[] args) throws IOException{
        //let's first make sure the right number of arguments is given in
        if (args.length!=2){
            System.out.println("Written by Anushka Sheoran");
            System.exit(1);//I exit the program here
        }

        int port_number = Integer.parseInt(args[0]);   //Here I process the port number and directory and make variables
        String dir = args[1];

        ServerSocket listening_sock = new ServerSocket(port_number); //We create a socket with that socket number, that keeps listening for new ports

        while(true){
            Socket sock = listening_sock.accept();//we allow that socket to accept incoming messages
            logger.info("connection from:" +sock.getRemoteSocketAddress()); //we allow that socket to accept incoming messages
            new Thread(() -> {
                try {
                    actuallyServing((sock), dir);
                } catch (Exception e) {
                    logger.error("Error serving: " + e.getMessage());
                } finally{
                    try{sock.close();} catch (IOException ignored) {}
                }
            }).start();

        }
    }
    public static void actuallyServing (Socket sock, String dir) throws IOException {
        InputStream in = sock.getInputStream();  //info is going from the socket to this in stream
        OutputStream outputstream = sock.getOutputStream();
        boolean ongoing = true;
        while(ongoing){
            byte[] buf = new byte[8000]; //basically here create a buffer in which the incoming data/message can be read, 8000 indices in an empty array
            int full_read_len = 0; //will use below
            byte[] headerBytes = null;//i am temporarily setting headerbytes to null
            //Okay, this was previously just making the incoming data into a string, instead I need to keep it as bytes
            while(true){
                int n = in.read(buf, full_read_len, buf.length-full_read_len); //in.read gives the number of bytes read in, and it can come in many chunks hence the loop
                if (n==-1) break; //when the client has finished sending in.read gives -1
                full_read_len+=n;
                //in the for loop below, we find out where the headers end    
                for (int i=3; i<full_read_len;i+=1){
                    if(buf[i-3]==13 &&buf[i-2]==10&&buf[i-1]==13&&buf[i]==10){ //we copy the header piece in headerBytes
                        headerBytes = Arrays.copyOfRange(buf, 0, i);//we read the headerBytes and extrac the first line and the rest of the headers, the basic logic is it goes from bytes, to characters to per line
                    }
                }
                if (headerBytes!=null){
                    break;
                }
            }
            
            if (headerBytes ==null){
                logger.warn("No headers found");
                return;
            }

            BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(headerBytes))); //headerbytes is a byte array [71, 60 ..], which is converted into ['G'], 'E', then the buffered reader reads each line at a tiem
            String requestLine = reader.readLine(); //the first request line
            logger.info("Request line: "+requestLine); //logs the request line each time

            //here we are just splitting up the request line
            String[] request_line_parts = requestLine.split(" ");
            String method=request_line_parts[0];
            String url=request_line_parts[1];
            String version = request_line_parts[2];
            
            //since we have the request line, we can break that apart and check for a few errors:
            if (request_line_parts.length!=3){
                showError(sock, 400, "Bad Request");
                return;
            }
            if (!version.equals("HTTP/1.1")){
                showError(sock, 400, "Bad Request");
                return;
            }
            if("POST".equals(request_line_parts[0])||"PUT".equals(request_line_parts[0])){
                showError(sock, 405, "Not Allowed");
                return;
            }
            if(!method.equals("PUT")&&!method.equals("POST")&&!method.equals("GET")&&!method.equals("HEAD")){
                showError(sock, 501, "Not Implemented");
                return;
            }
            if (!"HTTP/1.1".equals(request_line_parts[2])){
                showError(sock, 505, "Version Not Supported");
                return;
            }
            if (url.contains("..")){
                showError(sock, 403, "Forbidden");
                return;
            }

            //lets check some errors about the file
            File file = new File(dir, url); //finds the file
            if (!file.exists()){
                showError(sock, 404, "Not Found");
                return;
            }
            if (!file.canRead()) {
                showError(sock, 403, "Forbidden");
                return;
            }

            String line;
            int contentLength =0;
            boolean hostheader = false;
            String closeHeader = " ";
            while((line = reader.readLine())!= null &&!line.isEmpty()){
                logger.info("header: "+line);
                if (line.startsWith("Content-Length:")){
                    contentLength = Integer.parseInt(line.substring(15).trim()); //basically underneath the we just get the content length number, which is from the 15th index onward
                }
                if (line.startsWith("Host")){
                    hostheader = true;
                }
                if (line.toLowerCase().startsWith("connection")){
                    closeHeader=line.toLowerCase();
                }
            }
            if (hostheader==false){
                showError(sock, 400, "Bad Request");
                return;
            }
            //THIS JUST READS THE MESSAGE, CURRENTLY WE ARE NOT DOING ANYTHING WITH IT!
            if (contentLength>0) {
                byte[] body_message = new byte[contentLength];//here we basically create an array that stores the length of the body message
                int totalRead =0;
                while(totalRead<contentLength){ //here we read into the buffer the message
                    int n = in.read(body_message, totalRead, contentLength-totalRead);
                    if (n==-1) break;
                    totalRead+=n;
                }
            }
            //THIS IS STEP THREE IN WHICH WE ARE GOING TO ACTUALLY SEND A  RESPONSE BACK!
            if (method.equals("GET")||method.equals("HEAD")){
                String headers = "HTTP/1.1 200 OK\r\n" + "Content-Type: " + guessContentType(file) + "\r\n" +"Server: cis5550.webserver.Server\r\n" +"Content-Length: " + file.length() + "\r\n" +"\r\n"; // two indents at the end
                outputstream.write(headers.getBytes() );
                outputstream.flush();
            }

            //now we send the actual file message
            if (method.equals("GET")){
                FileInputStream filestream = new FileInputStream(file);
                byte[] buf2 = new byte [8000];
                int a;
                while ((a=filestream.read(buf2))!=-1){         //same logic we been using, write it into the buffer until we reach the end
                    outputstream.write(buf2,0,a);
                }
                outputstream.flush();
                filestream.close();       
            }   
        if (closeHeader.contains("close")){
            ongoing= false;
        }     
        }
        
    }
}
