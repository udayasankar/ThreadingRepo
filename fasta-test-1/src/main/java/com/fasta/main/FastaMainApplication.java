package com.fasta.main;

import com.fasta.serviceimpl.FileProcessorServiceImpl;
import com.fasta.utils.FastaGetPropertyValues;

public class FastaMainApplication {
	   public static void main(String[] args)
	   {
		   FileProcessorServiceImpl fileService;
			try {
				if(args.length>0)
				{
					FastaGetPropertyValues properties = new FastaGetPropertyValues();
					properties.getPropValues();
					fileService = new FileProcessorServiceImpl("");
					fileService.processFile(args);
				}
				else
				{
					System.out.println("Please enter the files name to extract");
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	   }
}
