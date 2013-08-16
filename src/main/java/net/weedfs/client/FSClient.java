package net.weedfs.client;

import java.io.File;
import java.io.InputStream;


public interface FSClient {
    
    public RequestResult download(String fid, String path);
    
    public InputStream download(String fid);
    
    public RequestResult upload(File inputFile);
    
    RequestResult upload(byte[] data, String fileName, String mimeType);
    
    RequestResult upload(InputStream inputstream, String fileName, String mimeType);
}
