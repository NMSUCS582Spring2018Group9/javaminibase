package tests;

import java.io.*;
import java.util.*;
import java.lang.*;
//import heap.*;
//import bufmgr.*;
//import diskmgr.*;
import global.*;
import heap.Heapfile;
import heap.Tuple;
import columnar.*;
import diskmgr.*;
//import chainexception.*;


class CFDriver extends TestDriver implements GlobalConst
{

  private final static boolean OK = true;
  private final static boolean FAIL = false;
  
  private int choice;
  private final static int reclen = 32;
  
  public CFDriver () {
    super("cntest");
    choice = 100;      // big enough for file to occupy > 1 data page
    //choice = 2000;   // big enough for file to occupy > 1 directory page
    //choice = 5;
  }
  

public boolean runTests () {

    System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");

    // set pages size to 0 so a DB is opened instead of creating a new one
    //SystemDefs sysdef = new SystemDefs(dbpath,0,100,"Clock");
   
    // Kill anything that might be hanging around
    String remove_logcmd;
    String remove_dbcmd;
    String remove_cmd = "/bin/rm -rf ";
    
    remove_logcmd = remove_cmd + logpath;
    remove_dbcmd = remove_cmd + dbpath;
    
    // Commands here is very machine dependent.  We assume
    // user are on UNIX system here
//    try {
//      Runtime.getRuntime().exec(remove_logcmd);
//      Runtime.getRuntime().exec(remove_dbcmd);
//    }
//    catch (IOException e) {
//      System.err.println ("IO error: "+e);
//    }
    
    //Run the tests. Return type different from C++
    boolean _pass = runAllTests();
    
    //Clean up again
//    try {
//      Runtime.getRuntime().exec(remove_logcmd);
//      Runtime.getRuntime().exec(remove_dbcmd);
//    }
//    catch (IOException e) {
//      System.err.println ("IO error: "+e);
//    }
    
    System.out.print ("\n" + "..." + testName() + " tests ");
    System.out.print (_pass==OK ? "completely successfully" : "failed");
    System.out.print (".\n\n");
    
    return _pass;
  }

	protected String testName() { 
		return "*** Columnar file ***"; 
  }

	protected boolean test1()
	{		
		System.out.println ("\n  Test 1: Creating and deleting a column-based file\n");
		boolean status = OK;
		
		// create database with 100 pages and 100 buffers
		SystemDefs sysdef = new SystemDefs(dbpath,100,100,"Clock");
				
		String fileName = "columnar_file_1"; 
		
		AttrType[] attributes = new AttrType[3];
		attributes[0] = new AttrType(AttrType.attrInteger);
		attributes[1] = new AttrType(AttrType.attrString);
		attributes[2] = new AttrType(AttrType.attrString);
		
		Columnarfile f = null;
		//SystemDefs.JavabaseDB.printEntries();
		try {
			f = new Columnarfile(fileName, attributes.length, attributes);
		}catch(Exception e) {
			status = false;
			System.out.println("error: " + e.getMessage());
		}
		
		try {
			SystemDefs.JavabaseBM.flushAllPages();
			SystemDefs.JavabaseDB.closeDB();
		}catch(Exception e) {/*empty*/}
		
		try {
			// reopen db created previously
			sysdef = new SystemDefs(dbpath,0,100,"Clock");
			
			sysdef.JavabaseDB.printEntries();
			
			// files entries should be there in database header page
			if(sysdef.JavabaseDB.get_file_entry(fileName+".hdr") == null)
				status = false;
			if(sysdef.JavabaseDB.get_file_entry(fileName+".0") == null)
				status = false;
			if(sysdef.JavabaseDB.get_file_entry(fileName+".1") == null)
				status = false;
			if(sysdef.JavabaseDB.get_file_entry(fileName+".2") == null)
				status = false;
			
			f.deleteColumnarFile();
			
			// no entries should be found now
			if(sysdef.JavabaseDB.get_file_entry(fileName+".hdr") != null)
				status = false;
			if(sysdef.JavabaseDB.get_file_entry(fileName+".0") != null)
				status = false;
			if(sysdef.JavabaseDB.get_file_entry(fileName+".1") != null)
				status = false;
			if(sysdef.JavabaseDB.get_file_entry(fileName+".2") != null)
				status = false;
		}catch(Exception e) {
			status = false;
			System.out.println("Exception: "+ e.getMessage());
		}
		
		return status;
	}
	
	protected boolean test2()
	{		
		System.out.println ("\n  Test 2: Inserting and reading tuples \n");
		boolean status = OK;
		
		// create database with 100 pages and 100 buffers
		SystemDefs sysdef = new SystemDefs(dbpath,100,100,"Clock");
				
		String fileName = "columnar_file_1"; 
		
		AttrType[] attributes = new AttrType[4];
		attributes[0] = new AttrType(AttrType.attrInteger);
		attributes[1] = new AttrType(AttrType.attrString);
		attributes[2] = new AttrType(AttrType.attrString);
		attributes[3] = new AttrType(AttrType.attrReal);
		
		Columnarfile f = null;
		//SystemDefs.JavabaseDB.printEntries();
		try {
			f = new Columnarfile(fileName, attributes.length, attributes);
			
			// records format is similar to the one in the sample test file Project-Phase2_test.txt
			// FORMAT: ID:Int Name:String Major:String Credit:Float
			int id = 100;
			String name = "Abdu";
			String major = "CS Unfortunately";
			float credit = 39;
			byte[] data = new byte[4 + 2 + 2 + 4 + name.length()*2 + major.length()*2];
			
			int pos = 0;
			Convert.setIntValue(id, pos, data);
			pos += 4;
			
			Convert.setShortValue((short)name.length(), pos, data);
			pos += 2;
			Convert.setStrValue(name, pos, data);
			pos += name.length()*2;
			
			Convert.setShortValue((short)major.length(), pos, data);
			pos += 2;
			Convert.setStrValue(major, pos, data);
			pos += major.length()*2;
			
			Convert.setFloValue(credit, pos, data);
			pos += 4;
			
			TID tid = f.insertTuple(data);
			
			// read the inserted tuple back
			f.getTuple(tid);
		}catch(Exception e) {
			status = false;
			System.out.println("error: " + e.getMessage());
		}
		
		try {
			SystemDefs.JavabaseBM.flushAllPages();
			SystemDefs.JavabaseDB.closeDB();
		}catch(Exception e) {/*empty*/}
		
		
		return status;
	}

}

public class CFTest {

   public static void main (String argv[]) {

     CFDriver hd = new CFDriver();
     boolean dbstatus;

     dbstatus = hd.runTests();

     if (dbstatus != true) {
       System.err.println ("Error encountered during buffer manager tests:\n");
       Runtime.getRuntime().exit(1);
     }

     Runtime.getRuntime().exit(0);
   }
}

