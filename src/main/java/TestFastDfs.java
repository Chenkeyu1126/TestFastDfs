
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
 
import org.csource.common.NameValuePair;
import org.csource.fastdfs.ClientGlobal;
import org.csource.fastdfs.FileInfo;
import org.csource.fastdfs.StorageClient;
import org.csource.fastdfs.StorageServer;
import org.csource.fastdfs.TrackerClient;
import org.csource.fastdfs.TrackerServer;
//import org.junit.Test;
 
public class TestFastDfs {
	
	//fdfs_client 核心配置文件
    public static String conf_filename = "src/main/resources/fdfs_client.conf"; 
 
    public static String[] testUpload(String local_filename ,String local_filetype) {	//上传文件
    	
    	String fileIds[] = new String[2];
    	TrackerServer trackerServer =null;
    	StorageServer storageServer = null;
    	    	
        try { 
            ClientGlobal.init(conf_filename);
            TrackerClient tracker = new TrackerClient(); 
            trackerServer = tracker.getConnection(); 
            
//            这个参数可以指定，也可以不指定，如果指定了，可以根据 testGetFileMate()方法来获取到这里面的值
//            NameValuePair nvp [] = new NameValuePair[]{ 
//                    new NameValuePair("age", "18")
//                    new NameValuePair("sex", "male") 
//            }; 
            
            StorageClient storageClient = new StorageClient(trackerServer, storageServer);
//          String fileIds[] = storageClient.upload_file(local_filename, "png", nvp);
            fileIds = storageClient.upload_file(local_filename, local_filetype, null);//扩展名         
            	
        } catch (Exception e) { 
            e.printStackTrace(); 
        } finally{
			try {
				if(null!=storageServer) storageServer.close();
				if(null!=trackerServer) trackerServer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        return fileIds;
    }
    
    public static void testDownload(String groupId, String filepath, String storePath) {	//下载文件
    	TrackerServer trackerServer =null;
    	StorageServer storageServer = null;
    	
        try {
            ClientGlobal.init(conf_filename);
            TrackerClient tracker = new TrackerClient(); 
            trackerServer = tracker.getConnection(); 
 
            StorageClient storageClient = new StorageClient(trackerServer, storageServer); 
            byte[] bytes = storageClient.download_file(groupId, filepath); 
            
            OutputStream out = new FileOutputStream(storePath);
            out.write(bytes);
            out.close();
        } catch (Exception e) { 
            e.printStackTrace(); 
        } finally {
			try {
				if(null!=storageServer) storageServer.close();
				if(null!=trackerServer) trackerServer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) { 
	            e.printStackTrace(); 
	        } 
        }
    }
   
    public static void testGetFileInfo(String groupId,String Filepath){ //获取文件信息
    	TrackerServer trackerServer =null;
    	StorageServer storageServer = null;
    
        try {
            ClientGlobal.init(conf_filename);
 
            TrackerClient tracker = new TrackerClient(); 
            trackerServer = tracker.getConnection(); 
 
            StorageClient storageClient = new StorageClient(trackerServer, storageServer); 
            FileInfo file = storageClient.get_file_info(groupId, Filepath); 
            
//            System.out.println("tracker_server ip--->"+file.getSourceIpAddr()); 
//            System.out.println("文件大小--->"+file.getFileSize()+"Byte"); 
//            System.out.println("文件上传时间--->"+file.getCreateTimestamp()); 
//            System.out.println(file.getCrc32()); 
        } catch (Exception e) { 
            e.printStackTrace(); 
        } finally{
			try {
				if(null!=storageServer) storageServer.close();
				if(null!=trackerServer) trackerServer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
       }
    } 

    public static void testGetFileMate(){ //获取文件的原数据类型
    	TrackerServer trackerServer =null;
    	StorageServer storageServer = null;
    	
    	try { 
    		String groupName = "group1";
        	String filePath = "M00/00/00/wKgbv1zmDVmAZMcGAABwDYuGgWA283.png";
    		ClientGlobal.init(conf_filename);
 
            TrackerClient tracker = new TrackerClient(); 
            trackerServer = tracker.getConnection(); 
 
            StorageClient storageClient = new StorageClient(trackerServer, 
                    storageServer); 
            
            //这个值是上传的时候指定的NameValuePair
            NameValuePair nvps [] = storageClient.get_metadata(groupName, filePath); 
            if(null!=nvps && nvps.length>0){
            	for(NameValuePair nvp : nvps){ 
            		System.out.println(nvp.getName() + ":" + nvp.getValue()); 
            	} 
            }
        } catch (Exception e) { 
            e.printStackTrace(); 
        } finally{
			try {
				if(null!=storageServer) storageServer.close();
				if(null!=trackerServer) trackerServer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
       }
    } 

    public static int testDelete(String groupId, String Filepath){ //删除文件
	TrackerServer trackerServer =null;
	StorageServer storageServer = null;
	int i=0;
	try {
		ClientGlobal.init(conf_filename);		
        TrackerClient tracker = new TrackerClient(); 
        trackerServer = tracker.getConnection(); 
      
        StorageClient storageClient = new StorageClient(trackerServer, storageServer); 
         i = storageClient.delete_file(groupId, Filepath); 
               
    } catch (Exception e) { 
        e.printStackTrace(); 
    } finally{
		try {
			if(null!=storageServer) storageServer.close();
			if(null!=trackerServer) trackerServer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
   }
	return i;
}

    public static void main(String[] args) {
	String[] downloadpath= testUpload("/D:\\哈希.png","png");
	for(int k =0;k< downloadpath.length;k++) {
		System.out.println(downloadpath[k]);
	}
	testDownload(downloadpath[0],downloadpath[1],"D:\\download.png");
	testGetFileInfo(downloadpath[0],downloadpath[1]);
	int ifdelete = testDelete(downloadpath[0],downloadpath[1]);
	 System.out.println( ifdelete==0 ? "删除成功" : "删除失败:"+ifdelete); 
	}
}


