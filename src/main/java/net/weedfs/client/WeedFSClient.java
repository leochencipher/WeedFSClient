package net.weedfs.client;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.AbstractContentBody;
import org.apache.http.entity.mime.content.ByteArrayBody;
import org.apache.http.entity.mime.content.InputStreamBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.util.EntityUtils;
import com.google.gson.Gson;


/*
 *  WeedFSClient Class
 */
public class WeedFSClient implements FSClient {
    
    // master address & port number
    private String masterAddress;
    private String masterPort;
    
    public WeedFSClient (String address, String port) {
        this.masterAddress = address;
        this.masterPort = port;
    }
    
    public WeedAssignedInfo seedAssignFidRequest() {
        WeedAssignedInfo assignedInfo = null;
        BufferedReader in = null;
        
        // 1. send assign request and get fid
        try {
            in = new BufferedReader(new InputStreamReader(sendHttpGetRequest("http://"
                    + this.masterAddress + ":" + this.masterPort + "/", "dir/assign",
                    "GET")));
            
            String inputLine;
            StringBuffer response = new StringBuffer();
            
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            Gson gson = new Gson();
            assignedInfo = gson.fromJson(response.toString(), WeedAssignedInfo.class);
            
        }
        catch (Exception e) {
            throw new RuntimeException(e.toString());
        }
        finally {
            try {
                if (in != null)
                    in.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        return assignedInfo;
    }
    
    @Override
    public RequestResult upload(InputStream inputstream, String url,
            String fileName, String mimeType) {
        WeedAssignedInfo assignedInfo = seedAssignFidRequest();
        return uploadGeneral(inputstream,  assignedInfo.getUrl(), url, fileName, mimeType);
    }
    
    
    @Override
    public RequestResult upload(byte[] data, String url, String fileName,
            String mimeType) {
        WeedAssignedInfo assignedInfo = seedAssignFidRequest();
        return uploadGeneral(data,  assignedInfo.getUrl(), url, fileName, mimeType);
    }
    
    
    @Override
    public RequestResult upload(File inputFile) {
        if (!inputFile.exists()) {
            throw new IllegalArgumentException("File doesn't exist");
        }
        
        WeedAssignedInfo assignedInfo = seedAssignFidRequest();
        InputStream inStream = null;
        try {
            inStream = new FileInputStream(inputFile);
        }
        catch (Exception e) {
            throw new RuntimeException(e.toString());
        }
        
        return upload(inStream, assignedInfo.getFid(), assignedInfo.getUrl(),
                inputFile.getName(), "text/plain");
    }
    
    
    /*
     * example: fid = 3,01637037d6 write file to local file
     */
    @Override
    public RequestResult download(String fid, String path) {
        
        if (fid == null || fid.length() == 0) {
            throw new IllegalArgumentException("Fid cannot be empty");
        }
        
        if (path == null || path.length() == 0) {
            throw new IllegalArgumentException("File path cannot be empty");
        }
        
        File output = new File(path);
        RequestResult result = new RequestResult();
        
        if (output.exists()) {
            throw new IllegalArgumentException("output file ");
        }
        
        BufferedReader in = null;
        
        // 2. download the file
        BufferedOutputStream wr = null;
        InputStream input = download(fid);
        
        try {
            output.createNewFile();
            wr = new BufferedOutputStream(new FileOutputStream(output));
            
            byte[] buffer = new byte[4096];
            int len = -1;
            while ((len = input.read(buffer)) != -1) {
                wr.write(buffer, 0, len);
            }
            result.setSuccess(true);
        }
        catch (Exception e) {
            throw new RuntimeException(e.toString());
        }
        finally {
            try {
                if (in != null)
                    in.close();
                if (wr != null)
                    wr.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }
    
    /*
     * example: fid = 3,01637037d6 write file to local file
     */
    @Override
    public InputStream download(String fid) {
        
        if (fid == null || fid.length() == 0) {
            throw new IllegalArgumentException("Fid cannot be empty");
        }
        
        String volumnId = fid.split(",")[0];
        ServerLocations locations = null;
        
        BufferedReader in = null;
        
        // 1. send quest to get volume address
        try {
            in = new BufferedReader(new InputStreamReader(sendHttpGetRequest("http://"
                    + this.masterAddress + ":" + this.masterPort + "/",
                    "dir/lookup?volumeId=" + volumnId, "GET")));
            String inputLine;
            StringBuffer response = new StringBuffer();
            
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            Gson gson = new Gson();
            locations = gson.fromJson(response.toString(), ServerLocations.class);
            
        }
        catch (Exception e) {
            throw new RuntimeException(e.toString());
        }
        finally {
            try {
                if (in != null) {
                    in.close();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        // 2. get input stream
        try {
            return sendHttpGetRequest("http://" + locations.getOnePublicUrl() + "/", fid,
                    "GET");
        }
        catch (Exception e) {
            throw new RuntimeException(e.toString());
        }
    }
    
    /*
     * delete the file
     */
    
    public RequestResult delete(String fid) {
        
        if (fid == null || fid.length() == 0) {
            throw new IllegalArgumentException("Fid cannot be empty");
        }
        
        RequestResult result = new RequestResult();
        
        String volumnId = fid.split(",")[0];
        ServerLocations locations = null;
        
        BufferedReader in = null;
        
        // 1. send quest to get volume address
        try {
            in = new BufferedReader(new InputStreamReader(sendHttpGetRequest("http://"
                    + this.masterAddress + ":" + this.masterPort + "/",
                    "dir/lookup?volumeId=" + volumnId, "GET")));
            String inputLine;
            StringBuffer response = new StringBuffer();
            
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            Gson gson = new Gson();
            locations = gson.fromJson(response.toString(), ServerLocations.class);
            
        }
        catch (Exception e) {
            throw new RuntimeException(e.toString());
        }
        finally {
            try {
                if (in != null)
                    in.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        // 2. delete the file
        try {
            
            HttpURLConnection con = null;
            URL requestUrl = new URL("http://" + locations.getOnePublicUrl() + "/" + fid);
            con = (HttpURLConnection) requestUrl.openConnection();
            
            con.setRequestMethod("DELETE");
            
            // add request header
            con.setRequestProperty("User-Agent", "");
            int responseCode = con.getResponseCode();
            
            if (responseCode == 200) {
                result.setSuccess(true);
            }
            else {
                result.setSuccess(false);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e.toString());
        }
        return result;
    }
    
    
    /*
     * Used to send request to WeedFS server
     */
    private InputStream sendHttpGetRequest(String host, String requestUrlDetail,
            String method) throws Exception {
        
        HttpURLConnection con = null;
        URL requestUrl = new URL(host.toString() + requestUrlDetail);
        con = (HttpURLConnection) requestUrl.openConnection();
        
        // optional default is GET
        con.setRequestMethod(method);
        
        // add request header
        con.setRequestProperty("User-Agent", "");
        // for later
        int responseCode = con.getResponseCode();
        return con.getInputStream();
    }
    
    
    private RequestResult uploadGeneral(Object data, String fid, String url,
            String fileName, String mimeType) {
        
        RequestResult result = new RequestResult();
        
        AbstractContentBody inputBody = null;
        
        try {
            if (Class.forName("java.io.InputStream").isInstance(data)) {
                inputBody = new InputStreamBody((InputStream) data, mimeType, fileName);
            }
            else {
                inputBody = new ByteArrayBody((byte[]) data, mimeType, fileName);
            }
        }
        catch (ClassNotFoundException e2) {
            e2.printStackTrace();
        }
        
        HttpClient client = new DefaultHttpClient();
        
        client.getParams().setParameter(CoreProtocolPNames.PROTOCOL_VERSION,
                HttpVersion.HTTP_1_1);
        
        HttpPost post = new HttpPost("http://" + url + "/" + fid);
        
        MultipartEntity entity = new MultipartEntity(HttpMultipartMode.BROWSER_COMPATIBLE);
        entity.addPart("fileBody", inputBody);
        try {
            entity.addPart("fileName", new StringBody(fileName));
        }
        catch (UnsupportedEncodingException e1) {
            e1.printStackTrace();
        }
        post.setEntity(entity);
        try {
            // TODO: add more file options;
            String response = EntityUtils.toString(client.execute(post).getEntity(),
                    "UTF-8");
            client.getConnectionManager().shutdown();
            int size = Integer.parseInt(response.substring(8, response.length() - 1));
            result.setFid(fid);
            result.setSize(size);
            result.setSuccess(true);
            return result;
        }
        catch (Exception e) {
            throw new RuntimeException(e.toString());
        }
    }
    
    
}
