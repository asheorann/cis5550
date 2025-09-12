package cis5550.webserver;
import cis5550.tools.Logger;
import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.util.Arrays;

//this is the package name and the public class Server is the main server that is accessed

//this is the main class because it hsa the public static void thing in it, the class is the blueprint and different methods are called
public class Server {
    //creating my ability to log
    private static final Logger logger = Logger.getLogger(Server.class);

    //this is my error response method, because i have to send it so many times
    private static void showError(Socket sock, int code, String message) throws IOException{
        OutputStream out = sock.getOutputStream();
        String response = "HTTP/1.1 " + code + " " + message + "\r\n" + "Content-Type: text/plain\r\n" +"Server: cis5550.webserver.Server\r\n" +"Content-Length: " + message.length() + "\r\n" +"\r\n" + message;
        out.write(response.getBytes());
        out.flush();
    }
    private static String guessContentType(File file) throws IOException {
        String type = Files.probeContentType(file.toPath());
        if (type == null) {
            return "application/octet-stream";
        }
        return type;
    }

    // private static String guessContentType(File file) {
    //     String name = file.getName().toLowerCase();
    //     if (name.endsWith(".html") || name.endsWith(".htm")){
    //         return "text/html";
    //     } 
    //     if (name.endsWith(".txt")) {
    //         return "text/plain";
    //     }
    //     if (name.endsWith(".png")){
    //         return "image/png";
    //     } 
    //     if (name.endsWith(".jpg") || name.endsWith(".jpeg")){
    //         return "image/jpeg";
    //     } 
    //     if (name.endsWith(".gif")) {
    //         return "image/gif";
    //     }
    //     return "unknown";
    // }

    public static void main(String[] args) throws IOException{
        //let's first make sure the right number of arguments is given in
        if (args.length!=2){
            System.out.println("Written by Anushka Sheoran");
            //I exit the program here
            System.exit(1);
        }

        //we make the port number an integer from the command line argument
        int port_number = Integer.parseInt(args[0]);
        String dir = args[1];
        //we create a socket with that socket number
        ServerSocket listening_sock = new ServerSocket(port_number);
        System.out.println("Server is running on port number "+port_number);

        while(true){
            //we allow that socket to accept incoming messages
            Socket sock = listening_sock.accept();

            //once we get something we say where it is coming from
            logger.info("connection from:" +sock.getRemoteSocketAddress());

            try {
                actuallyServing((sock), dir);
            } catch (Exception e) {
                logger.error("Error serving: " + e.getMessage());
            } finally{
                sock.close();
            }

        }
    }
    public static void actuallyServing (Socket sock, String dir) throws IOException {
           
        //info is going from the socket to this in stream
        InputStream in = sock.getInputStream();
        //basically here create a buffer in which the incoming data/message can be read, 2000 indices in an empty array
        byte[] buf = new byte[8000];
        int full_read_len = 0;
        //i am temporarily setting headerbytes to null
        byte[] headerBytes = null;
        //Okay, this was previously just making the incoming data into a string, instead I need to keep it as bytes
        while(true){
            int n = in.read(buf, full_read_len, buf.length-full_read_len);
            if (n==-1) break;
            full_read_len+=n;
            //in the for loop below, we find out where the headers end    
            for (int i=3; i<full_read_len;i+=1){
                if(buf[i-3]==13 &&buf[i-2]==10&&buf[i-1]==13&&buf[i]==10){
                    //we copy the header piece in headerBytes
                    headerBytes = Arrays.copyOfRange(buf, 0, i);
                    //we read the headerBytes and extrac the first line and the rest of the headers
                    //the basic logic is it goes from bytes, to characters to per line
                }
            }
            if (headerBytes!=null) break;
        }
        
        if (headerBytes ==null){
            logger.warn("No headers found");
            return;
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(headerBytes)));

        //the first request line
        String requestLine = reader.readLine();
        logger.info("Request line: "+requestLine);

        //here we are just splitting up the request line
        String[] request_line_parts = requestLine.split(" ");
        String method=request_line_parts[0];
        String url=request_line_parts[1];
        String version = request_line_parts[2];
        //since we have the request line, we can break that apart and check for a few errors:
        if (request_line_parts.length!=3){
            showError(sock, 400, "bad request");
            return;
        }
        if("POST".equals(request_line_parts[0])||"PUT".equals(request_line_parts[0])){
            showError(sock, 405, "Not Allowed");
            return;
        }
        if (!"HTTP/1.1".equals(request_line_parts[2])){
            showError(sock, 505, "Version Not Supported");
            return;
        }
        if (url.contains("..")){
            showError(sock, 403, "Forbidden");
        }

        //lets check some errors about the file
        //this finds the file
        File file = new File(dir, url);
        if (!file.exists()){
            showError(sock, 404, "Not Found");
        }

        String line;
        int contentLength =0;
        while((line = reader.readLine())!= null &&!line.isEmpty()){
            logger.info("header: "+line);
            if (line.startsWith("Content-Length:")){
                //basically underneath the we just get the content length number, which is from the 15th index onwards
                contentLength = Integer.parseInt(line.substring(15).trim());
            }
        }
        if (contentLength>0) {
            //here we basically create an array that stores the length of the body message
            byte[] body_message = new byte[contentLength];
            int totalRead =0;
            //here we read into the buffer the message
            while(totalRead<contentLength){
                int n = in.read(body_message, totalRead, contentLength-totalRead);
                if (n==-1) break;
                totalRead+=n;
            }
        }
        //THIS IS STEP THREE IN WHICH WE ARE GOING TO ACTUALLY SEND A DUMMY RESPONSE BACK!
        OutputStream outputstream = sock.getOutputStream();
        String headers = "HTTP/1.1 200 OK\r\n" + "Content-Type: " + guessContentType(file) + "\r\n" +"Server: cis5550.webserver.Server\r\n" +"Content-Length: " + file.length() + "\r\n" +"\r\n"; // two indents at the end
        outputstream.write(headers.getBytes() );
        outputstream.flush();
        //out.println("Hi omg i hope this works!"); // body --- lol this was my test message earlier

        //now we send the actual file message
        FileInputStream filestream = new FileInputStream(file);
       
        byte[] buf2 = new byte [8000];
        int a;
        //same logic we been using, write it into the buffer until we reach the end
        while ((a=filestream.read(buf2))!=-1){
            outputstream.write(buf2,0,a);
        }

        outputstream.flush();
        filestream.close();
      //  listening_sock.close();
        
    
    }
}
