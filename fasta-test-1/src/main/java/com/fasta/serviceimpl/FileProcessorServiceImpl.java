package com.fasta.serviceimpl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.fasta.utils.FastaGetPropertyValues;

public class FileProcessorServiceImpl implements Callable<String>{
	private static Integer count=1;
	private static ConcurrentHashMap<String, Integer> resultHashMap;
	private static ConcurrentHashMap<String, StringBuilder> sequenceHashMap;
	public String fileName;

	public FileProcessorServiceImpl(String fileName)
	{
	   this.fileName=fileName;	
	}
	
	public String processFile(String[] args) throws Exception
	{
		try
		{
			resultHashMap=new ConcurrentHashMap<String, Integer>();
			sequenceHashMap=new ConcurrentHashMap<String, StringBuilder>();
			ExecutorService esvc = Executors.newSingleThreadExecutor();
	
		    List<Future<String>> list = new ArrayList<Future<String>>();
		    FileProcessorServiceImpl[]	fileProcessImpl=new FileProcessorServiceImpl[args.length];
		    for (int i = 0; i < args.length; i++) {
		    	FileProcessorServiceImpl callable = new FileProcessorServiceImpl(args[i]);
		        fileProcessImpl[i]=callable;
		        Future<String> future = esvc.submit(callable);
	            list.add(future);
		    }
		   for(Future<String> fut : list){
		        	try {
		             	
		                System.out.println(new Date()+ "::"+fut.get());
		            } catch (InterruptedException | ExecutionException e) {
		                e.printStackTrace();
		            }
		        
		     }
			
		    BufferedWriter writer = new BufferedWriter(new FileWriter(FastaGetPropertyValues.prop.getProperty("foldername")+"REPORT.TXT"));
			if(resultHashMap.containsKey("FILE_CNT")) writer.write("FILE_CNT "+resultHashMap.get("FILE_CNT").toString()+"\n");
			if(resultHashMap.containsKey("SEQUENCE_CNT")) writer.write("SEQUENCE_CNT "+resultHashMap.get("SEQUENCE_CNT").toString()+"\n");
			if(resultHashMap.containsKey("BASE_CNT")) writer.write("BASE_CNT "+resultHashMap.get("BASE_CNT").toString()+"\n");
			TreeMap<String,String> alphaMap=new TreeMap<String, String>();
			resultHashMap.forEach((k,v)->{
			      if(!k.equals("FILE_CNT") && !k.equals("SEQUENCE_CNT") && !k.equals("BASE_CNT")) alphaMap.put(k, v.toString());
			});
			 alphaMap.forEach((k,v)->{
				   try {
					writer.write(k+" "+v+"\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
				});
			   writer.close();
			   
			   BufferedWriter writerSeq = new BufferedWriter(new FileWriter(FastaGetPropertyValues.prop.getProperty("foldername")+"SEQUENCE.FASTA.TXT")); 	   
			   TreeMap<Integer,StringBuilder> sortMap=new TreeMap<Integer, StringBuilder>();
			   sequenceHashMap.forEach((k, v) ->  {
				   String ttk=k.substring(1);
				   if(sortMap.containsKey(Integer.parseInt(ttk))) sortMap.put(Integer.parseInt(ttk), v);
				   else
				   	   sortMap.put(Integer.parseInt(ttk), v);
				   
				   
			   });   
			   
			 sortMap.forEach((k, v) -> {
				   try {
					writerSeq.write(">"+k+"\n");
					writerSeq.write(v+"\n");
			    
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				  
			});  
			writerSeq.close(); 
			 compressGzipFile(FastaGetPropertyValues.prop.getProperty("foldername")+"SEQUENCE.FASTA.TXT",
					   FastaGetPropertyValues.prop.getProperty("foldername")+"SEQUENCE.FASTA.GZ");
		    esvc.shutdown();
		}catch(Exception ex)
		{
			throw new Exception(ex);
		}
	    return "Success";
		
	}
	
	private static void compressGzipFile(String file, String gzipFile) throws Exception {
        try {
            FileInputStream fis = new FileInputStream(file);
            FileOutputStream fos = new FileOutputStream(gzipFile);
            GZIPOutputStream gzipOS = new GZIPOutputStream(fos);
            byte[] buffer = new byte[1024];
            int len;
            while((len=fis.read(buffer)) != -1){
                gzipOS.write(buffer, 0, len);
            }
            //close resources
            gzipOS.close();
            fos.close();
            fis.close();
        } catch (Exception ex) {
           throw new Exception(ex);
        }
        
    }

	public void  unGunzipFile() throws Exception {
		BufferedReader br = null;
		try {
				 	String fileNameExtract = FastaGetPropertyValues.prop.getProperty("foldername")+fileName;
			    	FileInputStream fileInputStream = new FileInputStream(fileNameExtract);
					GZIPInputStream zipInputstream = new GZIPInputStream(fileInputStream);
					String newFilename=FastaGetPropertyValues.prop.getProperty("foldername")+"filename_"+count+".txt";
			        OutputStream outStream = new FileOutputStream(newFilename);
			        final PrintStream printStream = new PrintStream(outStream);
			        printStream.println();
			        printStream.close();
			        if(!resultHashMap.containsKey("FILE_CNT"))
			        	resultHashMap.put("FILE_CNT", count);
					else
						resultHashMap.put("FILE_CNT", resultHashMap.get("FILE_CNT")+1);
					
			        count++;
			        FileOutputStream fileOutputStream = new FileOutputStream(newFilename);
					doCopy(zipInputstream, fileOutputStream); // copy and uncompress
					File fileRead = new File(newFilename);
					br = new BufferedReader(new FileReader(fileRead)); 
					String st,hashValue = null,keyValue = null;
					Integer seqCount=0, baseCount=0;
					while ((st = br.readLine()) != null) {
						  if(st.startsWith(">"))    
						  {
							  seqCount++; 
							  hashValue=st.substring(st.indexOf(".")+1);
							  keyValue=">"+hashValue;
						  }
	                	  else
	                	  {
	                		  baseCount=baseCount+st.length();
	                   		  if(!sequenceHashMap.containsKey(keyValue))
	                   			sequenceHashMap.put(keyValue,new StringBuilder(st));
	                   		  else
	                		  {
	                			  StringBuilder strBuild=sequenceHashMap.get(keyValue);
	                			  sequenceHashMap.put(keyValue,strBuild.append(st));
	                		  }
	                	  }
					    	for(int i=0;i<st.length();i++)
					    	{
					    	    char letter= st.charAt(i);
					    		if((letter >= 'A' && letter <= 'Z'))
						    		if (!resultHashMap.containsKey(Character.toString(letter)))
						    			resultHashMap.put(Character.toString(st.charAt(i)), 1);
								        else
								        	resultHashMap.put(Character.toString(st.charAt(i)), resultHashMap.get(Character.toString(st.charAt(i))) + 1);
							      
					    	}
					}
					
					if(!resultHashMap.containsKey("SEQUENCE_CNT"))
						resultHashMap.put("SEQUENCE_CNT", seqCount);
					else
						resultHashMap.put("SEQUENCE_CNT", resultHashMap.get("SEQUENCE_CNT")+seqCount);
					
					
								
					if(!resultHashMap.containsKey("BASE_CNT"))
						resultHashMap.put("BASE_CNT", baseCount);
					else
						resultHashMap.put("BASE_CNT", resultHashMap.get("BASE_CNT")+baseCount);
					
				}catch(Exception ex)
				{
				  throw new Exception(ex);
				}finally
				{
					br.close();
				}
	}
	
	public void doCopy(InputStream is, OutputStream os) throws Exception {
		int oneByte;
		while ((oneByte = is.read()) != -1) {
			os.write(oneByte);
		}
		os.close();
		is.close();
	}

	@Override
	public String call() throws Exception {
		unGunzipFile();
//		  System.out.println(fileName + " ended");
		    return fileName;
	}
}



