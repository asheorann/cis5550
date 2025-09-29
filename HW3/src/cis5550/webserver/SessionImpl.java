package cis5550.webserver;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SessionImpl implements Session{
    private String id;
    // System.currentTimeMillis().
    private long creationTime;
    private long lastAccessedTime;
    private int maxInactiveInterval;
    private boolean valid;
    private Map<String, Object> attributes;

    public SessionImpl(String id){
        this.id=id;
        this.creationTime = System.currentTimeMillis();
        this.lastAccessedTime = this.creationTime;
        this.maxInactiveInterval = 300;
        this.attributes = new ConcurrentHashMap<>();
        this.valid=true;
        
    }

    public String id(){
        return id;
    }
    public long creationTime(){
        return creationTime;
    }
    public long lastAccessedTime(){
        return lastAccessedTime;
    }
  // Set the maximum time, in seconds, this session can be active without being accessed.
  public void maxActiveInterval(int seconds){
    this.maxInactiveInterval=seconds;
  }
  

  // Invalidates the session. You do not need to delete the cookie on the client when this method
  // is called; it is sufficient if the session object is removed from the server.
  public void invalidate(){
    this.valid=false;
    this.attributes.clear();
  }
  public void access(){
    this.lastAccessedTime = System.currentTimeMillis();
  }

  // The methods below look up the value for a given key, and associate a key with a new value,
  // respectively.
  public Object attribute(String name){
    return this.attributes.get(name);
  }
  public void attribute(String name, Object value){
    this.attributes.put(name, value);
  }
  //no wi gotta add the method about cleaning up stuff, so is valid is the move
  public boolean isValid(){
    if(!this.valid){
        return false; //has it been invaliddated
    }
    if(this.maxInactiveInterval<0){
        return true;
    }
    long now=System.currentTimeMillis();
    boolean answer = (now-this.lastAccessedTime)<=(this.maxInactiveInterval*1000);
    return answer;
  }
}
