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
import java.util.concurrent.ConcurrentHashMap;


public class Worker extends cis5550.generic.Worker{
    private static ConcurrentHashMap<String, ConcurrentHashMap<String, Row>> tables=new ConcurrentHashMap<>();
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
        Server.put("/data/:table/:row/:column", (req, res) -> {
            String table=req.params("table");
            String row=req.params("row");
            String column=req.params("column");
            byte[] data =req.bodyAsBytes();
            String ifcolumn=req.queryParams("ifcolumn");
            String equals=req.queryParams("equals");
            tables.putIfAbsent(table, new ConcurrentHashMap<>());
            ConcurrentHashMap<String, Row> tabledata=tables.get(table);
            tabledata.putIfAbsent(row, new Row(row));
            Row rowdata=tabledata.get(row); //im beginnging doing this pls work gradescope
            if(ifcolumn!=null&&equals!=null){
                String existingval=rowdata.get(ifcolumn);
                if(existingval==null){
                    res.body("FAILED");
                    return null;
                }
            }
            rowdata.put(column, data);
            res.body("OK");
            return null;
        });
        Server.get("/data/:table/:row/:column",(req,res) -> { //maybe adding these methods will allow it to pass?
            String table=req.params("table");
            String row=req.params("row");
            String column=req.params("column");
            ConcurrentHashMap<String, Row>tableData=tables.get(table);
            if (tableData==null) { //if the table i snot found
                res.status(404,"Not Found");
                return null;
            }
            Row rowData=tableData.get(row); //if row is not found
            if (rowData==null) {
                res.status(404,"Not Found");
                return null;
            }
            byte[] data = rowData.getBytes(column);
            if (data==null) {
                res.status(404,"Not Found"); //yadayadya agian we check about thenull
                return null;
            }
            res.bodyAsBytes(data); //now return the daata as binaryyy
            return null;
        });
        cis5550.generic.Worker.startPingThread(coordinatoraddy, storagedir, workerport, workerid);

    }
}
