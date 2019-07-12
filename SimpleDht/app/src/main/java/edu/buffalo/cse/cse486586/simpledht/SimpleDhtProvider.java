package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

import static java.lang.Math.max;


/*********************************************
 *
 * @author  Malvika
 *
 */
//ChordNode for handling node joins
class ChordNode
{
    String hash;
    int emul_id;
    int port;
    ChordNode(String genhash,int id,int prt)
    {
        hash=genhash;
        emul_id=id;
        port=prt;
    }
}
/*
   1)https://stackoverflow.com/questions/683041/how-do-i-use-a-priorityqueue
           The above reference was used to understand how to implement the comparator
           of a priority queue and the same logic is implemented from the link

   2)https://docs.oracle.com/javase/7/docs/api/java/util/Comparator.html
    Reference documentation for understanding the implementation of a comparator

*/
//Ref - https://stackoverflow.com/questions/683041/how-do-i-use-a-priorityqueue
//Comparator to sort the chordnode values in arraylist based on the hash value of emulator-id
class ChordComp implements Comparator<ChordNode> {
    public int compare(ChordNode s1, ChordNode s2) {
        if (s1.hash.compareToIgnoreCase(s2.hash) < 0 )
        {
            return -1;
        }
        else if (s1.hash.compareToIgnoreCase(s2.hash) > 0)
        {
            return 1;
        }
        else
        {
            return 0;
        }

    }
}
/* References

   1)https://docs.oracle.com/javase/8/docs/api/java/util/HashMap.html
   The above link was used to understand the implementation of HashMap and its member functions
   Hashmap is used to store port as key and emultaor id as value

   2) https://docs.oracle.com/javase/7/docs/api/java/util/ArrayList.html
   The above link was used to understand the implementation of Arraylist

   3)https://docs.oracle.com/javase/7/docs/api/java/util/Collections.html
   The above link was used to understand sorting of arraylist for Java versions before 8 and how to sort with comparator

   4)https://stackoverflow.com/questions/8725387/why-is-there-no-sortedlist-in-java
   The above link was used to learn about sorted data structures in Java and learnt that List can be sorted from the
   Collections sort method


 */
public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();

    HashMap<String,String> EMUL_PORTS=new HashMap<String, String>(5);

    List<ChordNode> chordlist=new ArrayList<ChordNode>();
    ChordComp cmp=new ChordComp();

    static final int SERVER_PORT = 10000;

    //The below variables are used for information about successor,predecessor and about the head node
    int successor_port=0,predecessor_port=0,my_port=0;
    Boolean ishead=false;

    //Declaring the key and value field
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";


    private void insertChordList(ChordNode newchordnode)
    {
        //Ref: https://stackoverflow.com/questions/8725387/why-is-there-no-sortedlist-in-java
        chordlist.add(newchordnode);
        Collections.sort(chordlist,cmp);
        int succ_port=0,pred_port=0;
        int len=chordlist.size();
        String head="-Head:False";
        for(int i=0;i<chordlist.size();i++)
        {
            ChordNode cn=chordlist.get(i);
            if(newchordnode.emul_id==cn.emul_id)
            {
                if(i==0)
                {
                    if(len>1) {
                        succ_port = chordlist.get(i + 1).port;
                        pred_port = chordlist.get(len - 1).port;
                        head = "-Head:True";
                    }else{
                        succ_port = cn.port;
                        pred_port = cn.port;
                        head = "-Head:True";
                    }
                }
                else if(i==len-1)
                {
                    succ_port=chordlist.get(0).port;
                    pred_port=chordlist.get(i-1).port;
                }else
                {
                    succ_port=chordlist.get(i+1).port;
                    pred_port=chordlist.get(i-1).port;
                }
                break;
            }
        }


        //New Join Nodes Successor and Predecessor
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,Integer.toString(newchordnode.port), "Successor Node:".concat(Integer.toString(succ_port)));


        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,Integer.toString(newchordnode.port), "Predecessor Node:".concat(Integer.toString(pred_port)).concat(head));

        //Sending Successor details of new node's Predecessor
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,Integer.toString(pred_port),"Successor Node:".concat(Integer.toString(newchordnode.port)));

        //Also checking if it's the head
        if(succ_port==chordlist.get(0).port)
        {
            head="-Head:True";
        }
        else
        {
            head="-Head:False";
        }
        //Sending Predecessor details of new node's sucessor
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,Integer.toString(succ_port),"Predecessor Node:".concat(Integer.toString(newchordnode.port)).concat(head));
    }



    //Uri Builder module to build the uri in the required format for Content Provider
    //The module is taken from the OnPTestClickListener Code
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
/*
   REFERENCES FOR DELETE:
   1) https://developer.android.com/reference/java/io/File.html#delete()
   The above link was used to understand the delete function of files
   2)https://developer.android.com/reference/java/io/File.html#listFiles()
   The above link was used as reference to understand the function that lists the directory and respective files in that directory
   3)https://developer.android.com/reference/android/content/Context
   The above link was used to learn about the method deleteFile that's in context
 */
    private void delfiles()
    {
        File op[] = getContext().getFilesDir().listFiles();
        //Log.i("FILE DIR", Integer.toString(op.length));
        for (int i = 0; i < op.length; i++) {
            if (op[i].exists()) {
                op[i].delete();
                Log.i("File Deleted", op[i].getAbsolutePath());
            }
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        String originport=Integer.toString(my_port);
        if(selectionArgs!=null)
        {
            originport=selectionArgs[0];
        }
        if (selection.contentEquals(("@"))) {
             delfiles();
        } else if (selection.contentEquals("*")) {
            //to implement delete on all content providers from the list
             delfiles();
             if(Integer.parseInt(originport)!=successor_port)
             {
                    String delmes="Delete".concat(selection).concat(";Origin_Port:").concat(originport);
                   // Log.i(TAG,delmes);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,Integer.toString(successor_port),delmes);

             }

        }else {
            if (my_port == successor_port && successor_port == predecessor_port) {
                try {
                        getContext().deleteFile(selection);
                        Log.i("File Deleted",selection);
                } catch (Exception e) {
                    Log.e("Error", "File not found");
                }

            } else {
                try {
                    String idhash = genHash(selection);
                    String predhash = genHash(EMUL_PORTS.get(Integer.toString(predecessor_port)));
                    String myhash = genHash(EMUL_PORTS.get(Integer.toString(my_port)));
                    if ((idhash.compareToIgnoreCase(predhash) > 0 && idhash.compareToIgnoreCase(myhash) <= 0) || (idhash.compareToIgnoreCase(predhash) > 0 && ishead) || (idhash.compareToIgnoreCase(predhash) < 0 && idhash.compareToIgnoreCase(myhash) <= 0 && ishead)) {
                        try {

                            getContext().deleteFile(selection);
                            Log.i("File Deleted",selection);

                        } catch (Exception e) {
                            Log.e("Error", "File not found");
                        }


                    } else {

                        if(Integer.parseInt(originport)!=successor_port)
                        {
                            String delmess = "Delete:Key-".concat(selection).concat(";Origin_Port:").concat(originport);
                           // Log.i("Delete", delmess);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(successor_port), delmess);

                        }
                    }

                } catch (Exception e) {
                    Log.e(TAG, "Hash Function error");
                }
            }
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    private void insertfiles(String value,String filename)
    {
        FileOutputStream outputStream;
        try {
            outputStream = getContext().openFileOutput(filename,Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            outputStream.close();
        } catch (Exception e) {
            Log.e("Error", "File write failed");
        }


    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
               /*
           *  REFERENCES:
              ------------
              1)PA - 1 to learn how to use files and use them
              2)https://developer.android.com/reference/android/content/Context
                 To understand the concept of context and for using openFileOutput, and also to check various write
                 modes which provides delete contents and overwrite(mode is MODE_PRIVATE)

       */
        //Values are retrieved from the keys(which is key and value) and written to the file
        String value = values.getAsString(VALUE_FIELD);
        String filename = values.getAsString(KEY_FIELD);
        try {
            String idhash = genHash(filename);
            String predhash=genHash(EMUL_PORTS.get(Integer.toString(predecessor_port)));
            String myhash=genHash(EMUL_PORTS.get(Integer.toString(my_port)));
            if((idhash.compareToIgnoreCase(predhash)>0 && idhash.compareToIgnoreCase(myhash)<=0) || (idhash.compareToIgnoreCase(predhash)>0 && ishead) || (idhash.compareToIgnoreCase(predhash)<0 && idhash.compareToIgnoreCase(myhash)<=0 && ishead))
            {
                insertfiles(value,filename);
                Log.v("Insert", values.toString());
                return uri;
            }
            else if(successor_port==predecessor_port && my_port==successor_port)
            {
                insertfiles(value,filename);
                Log.v("Insert", values.toString());
                return uri;
            }
            else
            {
                String insertdata="INSERT:KEY-".concat(filename).concat(";VALUE-").concat(value);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,Integer.toString(successor_port),insertdata);
            }
        }catch(Exception e)
        {
            Log.e(TAG,"Hash Function Error");
        }

        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        //Initialising Hash Map
        EMUL_PORTS.put("11108","5554");
        EMUL_PORTS.put("11112","5556");
        EMUL_PORTS.put("11116","5558");
        EMUL_PORTS.put("11120","5560");
        EMUL_PORTS.put("11124","5562");

        final Uri mUri= buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");
        delete(mUri,"@",null);

        //To get the information about the current AVD port from the telephony manager
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        my_port=Integer.parseInt(myPort);

        //Create a server task which listens on port 10000
        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);


            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }

        //Sending Join Request to 5554
        if(EMUL_PORTS.get(myPort).contentEquals("5554"))
        {
            try {
                ChordNode temp = new ChordNode(genHash("5554"), 5554, Integer.parseInt(myPort));
                insertChordList(temp);
            }catch(Exception e)
            {
                Log.e(TAG,"Hash Algorithm Exception");
            }
        }else
        {
            //Send Request to 5554
            String message="JOIN:PORT-".concat(myPort).concat(";EMULATOR-").concat(EMUL_PORTS.get(myPort));
            successor_port=Integer.parseInt(myPort);
            predecessor_port=Integer.parseInt(myPort);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,Integer.toString(11108),message);

        }

        return false;
    }

    private MatrixCursor queryfiles()
    {
        String[] columnNames={KEY_FIELD,VALUE_FIELD};
        MatrixCursor mc=new MatrixCursor(columnNames);
        File op[] = getContext().getFilesDir().listFiles();
        for (int i = 0; i < op.length; i++) {
            if (op[i].exists()) {
                try {
                    FileInputStream in = getContext().openFileInput(op[i].getName());
                    int n = in.available();
                    byte[] result = new byte[n];
                    in.read(result);
                    String value = new String(result);
                    mc.addRow(new Object[]{op[i].getName(), value});
                }catch (Exception e)
                {
                    Log.e(TAG,"File not Found");
                }
                Log.v("Query", op[i].getName());
            }
        }
        return mc;
    }
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub

        /*
         *  REFERENCES:
         *   ------------
         *   1)PA - 1 to learn how to use files
         *   2)https://developer.android.com/reference/android/content/Context
         *   3)https://developer.android.com/reference/android/content/Context.html#openFileInput(java.lang.String)
         *     For using openFileInput and the return object associated with it
         *   4)https://developer.android.com/reference/java/io/FileInputStream.html
         *     To understand FileInputStream and its member functions,to read the file contents which is in bytes
         *     and convert them to string.
         *   5)https://developer.android.com/reference/java/io/FileInputStream.html#available()
         *     The available method returns the number of bytes available to read
         *     and the byte array is converted to string
         *   6)http://developer.android.com/reference/android/database/MatrixCursor.html
         *     To understand how to use matrix cursor and the function addRow

         */

        String[] columnNames={KEY_FIELD,VALUE_FIELD};
        MatrixCursor mc=new MatrixCursor(columnNames);
        String orgport=Integer.toString(my_port);
        if(sortOrder!=null)
        {
            orgport=sortOrder;
        }


        if(selection.contentEquals("@"))
        {
            mc=queryfiles();
            return mc;
        }
        else if(selection.contentEquals("*"))
        {
            if(successor_port==predecessor_port && my_port==successor_port)
            {
                mc=queryfiles();
                return mc;
            }else
            {

                MatrixCursor mcQuery = new MatrixCursor(columnNames);
                if(Integer.parseInt(orgport)!=successor_port) {
                    String querymess = "QUERY".concat(selection).concat(";Origin_Port:").concat(orgport);
                    //Log.i("QUERY", querymess);
                    List<String> reslist = null;
                    try {
                        // Ref: https://stackoverflow.com/questions/14045765/waiting-for-asynctask-to-finish-or-variable-to-be-set
                        reslist = new QueryStarTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(successor_port), querymess).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                    if(reslist!=null) {
                        for (int i = 0; i < reslist.size(); i++) {
                            String msgRcvd = reslist.get(i);
                            if (msgRcvd.contains("StarQuery:")) {
                              //  Log.i("RESULT", msgRcvd);
                                String reskey = msgRcvd.split("Key-")[1].split(";")[0];
                                String resval = msgRcvd.split("Value-")[1];
                                mcQuery.addRow(new Object[]{reskey, resval});
                            }

                        }
                    }
                }
                File op[] = getContext().getFilesDir().listFiles();
                for (int i = 0; i < op.length; i++) {
                    if (op[i].exists()) {
                        try {
                            FileInputStream in = getContext().openFileInput(op[i].getName());
                            int n = in.available();
                            byte[] result = new byte[n];
                            in.read(result);
                            String value = new String(result);
                            mcQuery.addRow(new Object[]{op[i].getName(), value});
                        }catch (Exception e)
                        {
                            Log.e(TAG,"File not Found");
                        }
                        Log.v("* Query", op[i].getName());
                    }
                }
                return mcQuery;
            }
        }
        else {
            if (my_port == successor_port && successor_port == predecessor_port) {
            try {
                    FileInputStream in = getContext().openFileInput(selection);
                    int n = in.available();
                    byte[] result = new byte[n];
                    in.read(result);
                    String value = new String(result);
                    mc.addRow(new Object[]{selection, value});
                    // Thread.sleep(1000);
                } catch(Exception e){
                    Log.e("Error", "File not found");
                }
                Log.v("Query", selection);
                return mc;
            }else
            {
                try {
                    String idhash = genHash(selection);
                    String predhash = genHash(EMUL_PORTS.get(Integer.toString(predecessor_port)));
                    String myhash = genHash(EMUL_PORTS.get(Integer.toString(my_port)));
                    if((idhash.compareToIgnoreCase(predhash)>0 && idhash.compareToIgnoreCase(myhash)<=0) || (idhash.compareToIgnoreCase(predhash)>0 && ishead) || (idhash.compareToIgnoreCase(predhash)<0 && idhash.compareToIgnoreCase(myhash)<=0 && ishead))
                    {
                        try {
                            FileInputStream in = getContext().openFileInput(selection);
                            int n = in.available();
                            byte[] result = new byte[n];
                            in.read(result);
                            String value = new String(result);
                            mc.addRow(new Object[]{selection, value});
                        } catch(Exception e){
                            Log.e("Error", "File not found");
                        }
                        Log.v("Query", selection);
                        return mc;

                    }else{

                        String querymess="QUERY:Key-".concat(selection).concat(";Origin_Port:").concat(orgport);

                        // Ref: https://stackoverflow.com/questions/14045765/waiting-for-asynctask-to-finish-or-variable-to-be-set
                        Cursor resq=new QueryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,Integer.toString(successor_port),querymess).get();
                        return resq;
                    }

                }catch(Exception e)
                {
                    Log.e(TAG,"Hash Function error");
                }

            }
        }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


    // The below code snippet is also taken from  PA-1 and PA-2A and PA-2B

    /************************************************************
     ---------------
     References Used
     ---------------

     1) https://docs.oracle.com/javase/tutorial/networking/sockets/clientServer.html
     The above reference was used to understand the concept of sockets,client,server sockets and how to implement
     input and output streams along with BufferedReader and PrintWriter respectively

     2) https://docs.oracle.com/javase/7/docs/api/java/net/ServerSocket.html
     The above reference was used to understand the methods available for ServerSocket and their functionality

     3) https://docs.oracle.com/javase/7/docs/api/java/net/Socket.html
     The above reference was used to understand the methods available for Socket and their functionality

     4) https://stackoverflow.com/questions/10240694/java-socket-api-how-to-tell-if-a-connection-has-been-closed/10241044#10241044
     The above reference was to understand the concept of socket closing and how to identify that with readLine

     5)https://docs.oracle.com/javase/7/docs/api/java/io/BufferedReader.html
     The above docs was used to understand the BufferedReader Class and their functionality

     6)https://docs.oracle.com/javase/7/docs/api/java/io/PrintWriter.html
     The above docs was used to understand the PrintWriter Class and their functionality

     7)https://docs.oracle.com/javase/tutorial/essential/concurrency/sleep.html
     The above docs was used to understand sleep in a thread

     OBJECT INPUT AND OUTPUT STREAM REFERENCES
     8)https://stackoverflow.com/questions/12895450/sending-an-arrayliststring-from-the-server-side-to-the-client-side-over-tcp-us
     The above link was used to understand the use of object input and output stream, the order in which it should be used and
     the way to pass arraylist as object to the streams and retrieve back

     9)https://docs.oracle.com/javase/7/docs/api/java/io/ObjectOutputStream.html
     The above documentation was used to know about the member functions of objectoutputstream

     10)https://docs.oracle.com/javase/7/docs/api/java/io/ObjectInputStream.html
     The above documentation was used to know about the member functions of objectinputstream

     MATRIX CURSOR REFERENCE
     11)https://developer.android.com/reference/android/database/MatrixCursor
     12)https://developer.android.com/reference/android/database/Cursor
     References for the use of  Matrix cursor and cursor implementations

     13)https://stackoverflow.com/questions/4396604/how-to-solve-cursorindexoutofboundsexception
     The above reference was to understand the exception thrown with matrixcursor and how to iterate through the
     values of the cursor using the movement of cursor pointer

     14)https://developer.android.com/reference/android/content/ContentValues
     The above link was used for the understanding of contentvalues

     ************************************************************************/

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            final Uri mUri= buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");
            try {
                //Server keeps listening for connections and accepting them and passing the message to Progress update
                while (true) {

                    //Accepts an incoming client connection
                    Socket client = serverSocket.accept();
                    //Reads the message from the input stream and sends to Progress Update through publish progress
                    BufferedReader input = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    String message = input.readLine();

                    if(message.contains("JOIN"))
                    {
                        PrintWriter outack= new PrintWriter(client.getOutputStream(), true);
                        outack.println("ACK");
                        int port=Integer.parseInt(message.split("-")[1].split(";")[0]);
                        int emulid=Integer.parseInt(message.split("EMULATOR")[1].split("-")[1]);
                        ChordNode temp=new ChordNode(genHash(Integer.toString(emulid)),emulid,port);
                        insertChordList(temp);
                    }
                    else if(message.contains("Successor Node:"))
                    {
                        PrintWriter outack= new PrintWriter(client.getOutputStream(), true);
                        outack.println("ACK");
                        successor_port=Integer.parseInt(message.split("Successor Node:")[1]);
                        Log.i("Success Port",Integer.toString(successor_port));
                    }else if(message.contains("Predecessor Node:"))
                    {
                        PrintWriter outack= new PrintWriter(client.getOutputStream(), true);
                        outack.println("ACK");
                        predecessor_port=Integer.parseInt(message.split("Predecessor Node:")[1].split("-")[0]);
                        Log.i("Predecessor Port",Integer.toString(predecessor_port));
                        ishead=Boolean.parseBoolean(message.split("Head:")[1]);
                        Log.i("IS head Node",Boolean.toString(ishead));
                    }
                    else if(message.contains("INSERT:"))
                    {
                        PrintWriter outack= new PrintWriter(client.getOutputStream(), true);
                        outack.println("ACK");
                        String key=message.split("KEY-")[1].split(";")[0];
                        String value=message.split("VALUE-")[1].split(";")[0];
                        final ContentValues mContentValues=new ContentValues();
                        mContentValues.put(KEY_FIELD,key);
                        mContentValues.put(VALUE_FIELD,value);
                        insert(mUri,mContentValues);
                        Log.i("Sucessive Insert",key);

                    }else if(message.contains("QUERY:"))
                    {
                        String key=message.split("Key-")[1].split(";")[0];
                        String originport=message.split("Origin_Port:")[1];
                        Cursor mcresult=query(mUri,null,key,null,originport);
                        if(mcresult!=null)
                        {
                            Log.i("MCresult",Integer.toString(mcresult.getCount()));

                            if(mcresult.moveToFirst()) {
                                int keyIndex = mcresult.getColumnIndex(KEY_FIELD);
                                int valueIndex = mcresult.getColumnIndex(VALUE_FIELD);
                                String returnKey = mcresult.getString(0);
                                String returnValue = mcresult.getString(valueIndex);

                                //Log.i("Return Key", returnKey);
                                //Log.i("Return Value", returnValue);
                                String queryresult = "ResultQuery:Key-".concat(returnKey).concat(";Value-").concat(returnValue);

                                Log.i("QUERY RES",queryresult);

                                PrintWriter outserver = new PrintWriter(client.getOutputStream(),true);
                                outserver.println(queryresult);

                                mcresult.close();
                            }
                        }


                    }
                    else if(message.contains("QUERY*"))
                    {

                        String originport=message.split("Origin_Port:")[1];
                        Cursor mcresult=query(mUri,null,"*",null,originport);
                        if(mcresult!=null)
                        {
                            Log.i("MCresult",Integer.toString(mcresult.getCount()));
                            List<String> curresult=new ArrayList<String>();

                            //Ref: https://stackoverflow.com/questions/4396604/how-to-solve-cursorindexoutofboundsexception
                            if (mcresult.moveToNext() && mcresult.getCount() >= 1) {
                                do {
                                    int keyIndex = mcresult.getColumnIndex(KEY_FIELD);
                                    int valueIndex = mcresult.getColumnIndex(VALUE_FIELD);
                                    String returnKey = mcresult.getString(0);
                                    String returnValue = mcresult.getString(valueIndex);
                                    //Log.i("Return Key", returnKey);
                                    //Log.i("Return Value", returnValue);
                                    String queryresult = "StarQuery:Key-".concat(returnKey).concat(";Value-").concat(returnValue);
                                    curresult.add(queryresult);
                                    Log.i("QUERY * RES", queryresult);
                                }while(mcresult.moveToNext());
                                mcresult.close();
                            }
                            //Ref: https://stackoverflow.com/questions/12895450/sending-an-arrayliststring-from-the-server-side-to-the-client-side-over-tcp-us
                            if(mcresult.getCount()>0) {
                                ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
                                oos.writeInt(mcresult.getCount());
                                oos.writeObject(curresult);
                                oos.flush();
                            }else
                            {
                                ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
                                oos.writeInt(mcresult.getCount());
                                oos.flush();
                            }
                        }else
                        {
                            Log.e(TAG,"CURSOR NULL");
                        }

                    }else if(message.contains("Delete*"))
                    {
                        PrintWriter outack= new PrintWriter(client.getOutputStream(), true);
                        outack.println("ACK");
                        String originport=message.split("Origin_Port:")[1];
                        delete(mUri,"*",new String[]{originport});
                    }else if(message.contains("Delete:"))
                    {
                        PrintWriter outack= new PrintWriter(client.getOutputStream(), true);
                        outack.println("ACK");
                        String filename=message.split("Key-")[1].split(";")[0];
                        String originport=message.split("Origin_Port:")[1];
                        delete(mUri,filename,new String[]{originport});
                    }

                    client.close();
                }
            } catch (Exception e) {
                Log.e(TAG, "Server couldn't accept a Client");
                Log.e(TAG, e.toString());
            }


            return null;
        }


    }

    // The below code snippet is also taken from PA-1 and PA-2A and PA-2B

    /************************************************************
     ---------------
     References Used
     ---------------
     1) https://docs.oracle.com/javase/tutorial/networking/sockets/clientServer.html
     The above reference was used to understand the concept of sockets,client,server sockets and how to implement
     input and output streams along with BufferedReader and PrintWriter respectively

     2) https://docs.oracle.com/javase/7/docs/api/java/net/ServerSocket.html
     The above reference was used to understand the methods available for ServerSocket and their functionality

     3) https://docs.oracle.com/javase/7/docs/api/java/net/Socket.html
     The above reference was used to understand the methods available for Socket and their functionality

     4) https://stackoverflow.com/questions/10240694/java-socket-api-how-to-tell-if-a-connection-has-been-closed/10241044#10241044
     The above reference was to understand the concept of socket closing and how to identify that with readLine

     5)https://docs.oracle.com/javase/7/docs/api/java/io/BufferedReader.html
     The above docs was used to understand the BufferedReader Class and their functionality

     6)https://docs.oracle.com/javase/7/docs/api/java/io/PrintWriter.html
     The above docs was used to understand the PrintWriter Class and their functionality

     7)https://docs.oracle.com/javase/tutorial/essential/concurrency/sleep.html
     The above docs was used to understand sleep in a thread

     OBJECT INPUT AND OUTPUT STREAM REFERENCES
     8)https://stackoverflow.com/questions/12895450/sending-an-arrayliststring-from-the-server-side-to-the-client-side-over-tcp-us
     The above link was used to understand the use of object input and output stream, the order in which it should be used and
     the way to pass arraylist as object to the streams and retrieve back

     9)https://docs.oracle.com/javase/7/docs/api/java/io/ObjectOutputStream.html
     The above documentation was used to know about the member functions of objectoutputstream

     10)https://docs.oracle.com/javase/7/docs/api/java/io/ObjectInputStream.html
     The above documentation was used to know about the member functions of objectinputstream

     MATRIX CURSOR REFERENCE
     11)https://developer.android.com/reference/android/database/MatrixCursor
     12)https://developer.android.com/reference/android/database/Cursor
     References for the use of  Matrix cursor and cursor implementations

     13)https://stackoverflow.com/questions/14045765/waiting-for-asynctask-to-finish-or-variable-to-be-set
     The above link was used to understand how to make asynctask return a value and to retrieve the result

     14)https://developer.android.com/reference/android/content/ContentValues
     The above link was used to understand content values

     ************************************************************************/

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            String remoteport = msgs[0];
            String msgToSend=msgs[1];
            try {

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(remoteport));
                //Sending the message to the receivers
                PrintWriter output0 = new PrintWriter(socket.getOutputStream(), true);
                output0.println(msgToSend);

                //Close socket if ack received from server
                BufferedReader inserver = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String msgRcvd = inserver.readLine();
                if(msgRcvd.contains("ACK"))
                {
                    socket.close();
                }
            }
            catch(Exception e)
            {
                Log.e(TAG,"Send Exception");
                Log.e(TAG,e.toString());
            }
            return null;
        }


    }

    private class QueryTask extends AsyncTask<String, Void, Cursor> {

        //Ref: https://stackoverflow.com/questions/14045765/waiting-for-asynctask-to-finish-or-variable-to-be-set
        @Override
        protected Cursor doInBackground(String... msgs) {

            String remoteport = msgs[0];
            String msgToSend=msgs[1];
            try {

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(remoteport));
                //Sending the message to the receivers
                PrintWriter output0 = new PrintWriter(socket.getOutputStream(), true);
                output0.println(msgToSend);
                //Thread.sleep(200);
                //Close socket if ack received from server
                BufferedReader inserver = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String msgRcvd = inserver.readLine();
                if(msgRcvd.contains("ResultQuery:"))
                {
                    Log.i("RESULT",msgRcvd);
                    String reskey=msgRcvd.split("Key-")[1].split(";")[0];
                    String resval=msgRcvd.split("Value-")[1];
                    String[] columnNames={KEY_FIELD,VALUE_FIELD};
                    MatrixCursor mcQuery=new MatrixCursor(columnNames);
                    mcQuery.addRow(new Object[]{reskey,resval});

                    socket.close();
                    return mcQuery;

                }



            }
            catch(Exception e)
            {
                Log.e(TAG,"Send Exception");
                Log.e(TAG,e.toString());
            }
            return null;
        }


    }

    private class QueryStarTask extends AsyncTask<String, Void, List<String>> {

        //Ref: https://stackoverflow.com/questions/14045765/waiting-for-asynctask-to-finish-or-variable-to-be-set
        @Override
        protected List<String> doInBackground(String... msgs) {

            String remoteport = msgs[0];
            String msgToSend=msgs[1];
            try {

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(remoteport));
                //Sending the message to the receivers
                PrintWriter output0 = new PrintWriter(socket.getOutputStream(), true);
                output0.println(msgToSend);


                //Ref: https://stackoverflow.com/questions/12895450/sending-an-arrayliststring-from-the-server-side-to-the-client-side-over-tcp-us

                ObjectInputStream oos = new ObjectInputStream(socket.getInputStream());
                int count=oos.readInt();
                List<String> reslist =null;
                if(count>0) {
                    Object resobject = oos.readObject();
                    reslist = (ArrayList<String>) resobject;
                }else{
                    reslist =null;
                }
                socket.close();
                return reslist;
            }
            catch(Exception e)
            {
                Log.e(TAG,"Send Exception");
                Log.e(TAG,e.toString());
            }
            return null;
        }


    }

}