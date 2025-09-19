package cis5550.webserver;
import java.io.OutputStream;
import java.util.*;

//wait come back to this

public class ResponseImpl implements Response {
    private int statusCode = 200;
    private String statusText = "OK";
    private Map<String, String> headers = new HashMap<>();
    private byte[] body = null;

    //now i am on the writeing step so i hav eto connect directly to the socket so need these new fields
    private OutputStream out;
    private boolean writtenheaders = false;
    
    public ResponseImpl(OutputStream out){
        this.out=out;
        headers.put("content-type", "text/html");
    }

    public void status (int code, String statusText){
        this.statusCode = code;
        this.statusText = statusText;
    }
    public void body(String s){
        //basically converting the string to bytes 
        if(s!=null){
            this.body=s.getBytes();
        }
    }
    public void bodyAsBytes(byte[] b){
        this.body = b;
    }
    public void header(String key, String value){
        if (key!=null && value!=null){
            headers.put(key.toLowerCase(), value);//ugh make sure that lowercase is the right thing
        }
    }
    public void type(String cType){
        header("content-type", cType);
    }
    public void redirect(String url, int rCode){
        //i dont really know why i am putting this but i keep getting an error that says I am not matching the response java but at least i put it there in a dummmy format
    }
    public void halt(int statusCode, String statusText){
        //same this as redirect i am putting it in as dummy and then later if i have time ill look at the extra credit
    }

    public void write(byte[] b) throws Exception {
        //first imma make sure that headers arent already written
        if (writtenheaders==false){
            String statusLine = "HTTP/1.1 " +statusCode + " " + statusText+ "\r\n";
            out.write(statusLine.getBytes());
            for (Map.Entry<String, String> entry : headers.entrySet()){
                String headLine = entry.getKey()+": "+ entry.getValue()+"\r\n";
                out.write(headLine.getBytes());
            }
            //as always gotta add another line to seperate headers and th ebody
            out.write("\r\n".getBytes());
            writtenheaders=true; // now i make em true
        }
        out.write(b); //this is the acc byte array
    }
    public boolean writtenheaders(){
        return writtenheaders;
    }
    public int getStatusCode() { return statusCode; }
    public String getStatusText() { return statusText; }
    public Map<String, String> getHeaders() { return headers; }
    public byte[] getBody() { return body; }
}
