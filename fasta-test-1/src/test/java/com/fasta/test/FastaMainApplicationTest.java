package com.fasta.test;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.junit.Test;

import com.fasta.serviceimpl.FileProcessorServiceImpl;
import com.fasta.utils.FastaGetPropertyValues;

public class FastaMainApplicationTest {
	
	   @Test
	   public void testFileProcess() throws Exception  {
		
		   FileProcessorServiceImpl fileProcessorService = new FileProcessorServiceImpl("");
		  FastaGetPropertyValues properties = new FastaGetPropertyValues();
			
				properties.getPropValues();
			
		  String[] args= {"1.fasta.gz","2.fasta.gz"};
	      assertNotNull("Success", fileProcessorService.processFile(args));
		 
	   }

}
