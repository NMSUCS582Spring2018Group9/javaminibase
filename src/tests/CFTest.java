package tests;

import java.io.*;
import java.util.*;

//import heap.*;

import global.*;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import columnar.*;


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
	
	// records format is similar to the one in the sample test file Project-Phase2_test.txt
	// FORMAT: ID:Int Name:String Major:String Credit:Float
	byte[] createRecord(int id, String name, String major, float credit)
	{
		byte[] data = new byte[4 + 2 + 2 + 4 + name.length()*2 + major.length()*2];
		
		int pos = 0;
		
		try {
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
		}catch(Exception e) {}
		
		return data;
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
		
		short[] stringsSizes = new short[2];
		stringsSizes[0] = 40;
		stringsSizes[1] = 40;
		
		Columnarfile f = null;
		//SystemDefs.JavabaseDB.printEntries();
		try {
			f = new Columnarfile(fileName, attributes.length, attributes, stringsSizes);
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
		System.out.println ("\n  Test 2: Inserting/reading/updating tuples and tuples count \n");
		boolean status = OK;
		
		// create database with 100 pages and 100 buffers
		SystemDefs sysdef = new SystemDefs(dbpath,100,100,"Clock");
				
		String fileName = "columnar_file_1"; 
		
		AttrType[] attributes = new AttrType[4];
		attributes[0] = new AttrType(AttrType.attrInteger);
		attributes[1] = new AttrType(AttrType.attrString);
		attributes[2] = new AttrType(AttrType.attrString);
		attributes[3] = new AttrType(AttrType.attrReal);
		
		short[] stringsSizes = new short[2];
		stringsSizes[0] = 40;
		stringsSizes[1] = 40;
		
		Columnarfile f = null;
		//SystemDefs.JavabaseDB.printEntries();
		try {
			f = new Columnarfile(fileName, attributes.length, attributes, stringsSizes);
			
			if(f.getTupleCnt()!= 0)
				throw new Exception("file's tuples count: expected: 0, actual: " + f.getTupleCnt());
			
			int id = 100;
			String name = "Abdu";
			String major = "CS Unfortunately";
			float credit = 39;
			
			Tuple t1 = new Tuple();
			t1.setHdr((short)4, attributes, stringsSizes);
			t1.setIntFld(1, id);
			t1.setStrFld(2, name);
			t1.setStrFld(3, major);
			t1.setFloFld(4, credit);
			
			TID tid = f.insertTuple(t1.getTupleByteArray());
			
			if(f.getTupleCnt()!= 1)
				throw new Exception("file's tuples count: expected: 1, actual: " + f.getTupleCnt());
			
			// prepare another record for insertion
			int id2 = 99;
			String name2 = "James";
			String major2 = "Engineering";
			float credit2 = 14;
			
			Tuple t2 = new Tuple();
			t2.setHdr((short)4, attributes, stringsSizes);
			t2.setIntFld(1, id2);
			t2.setStrFld(2, name2);
			t2.setStrFld(3, major2);
			t2.setFloFld(4, credit2);
			
			TID tid2 = f.insertTuple(t2.getTupleByteArray());
			
			if(f.getTupleCnt()!= 2)
				throw new Exception("file's tuples count: expected: 2, actual: " + f.getTupleCnt());
			
			// read the inserted tuple back
			t1 = f.getTuple(tid);
			if(t1.getIntFld(1) != id)
				status = false;
			if(!t1.getStrFld(2).equals(name))
				status = false;
			if(!t1.getStrFld(3).equals(major))
				status = false;
			if(Float.compare(t1.getFloFld(4), credit) != 0)
				status = false;
			
			t2 = f.getTuple(tid2);
			if(t2.getIntFld(1) != id2)
				status = false;
			if(!t2.getStrFld(2).equals(name2))
				status = false;
			if(!t2.getStrFld(3).equals(major2))
				status = false;
			if(Float.compare(t2.getFloFld(4), credit2) != 0)
				status = false;
			
			TID fakeTID = new TID(4);
			if(f.updateTuple(fakeTID, t1))
				status = false;
			
			
			id = 100;
			name = "Abdu";
			major = "CS";
			credit = 40;

			t1.setIntFld(1, id);
			t1.setStrFld(2, name);
			t1.setStrFld(3, major);
			t1.setFloFld(4, credit);
			
			f.updateTuple(tid, t1);
			
			Tuple t3 = f.getTuple(tid);
			Tuple t4 = f.getTuple(tid2);
			System.out.printf("%d, %s, %s, %.2f\n", t3.getIntFld(1), t3.getStrFld(2), t3.getStrFld(3), t3.getFloFld(4));
			System.out.printf("%d, %s, %s, %.2f\n", t4.getIntFld(1), t4.getStrFld(2), t4.getStrFld(3), t4.getFloFld(4));
			
			if(f.getTupleCnt() != 2)
				status = false;
			
			// update a single column (major column)
			AttrType[] type2 = new AttrType[1];
			short[] sSizes = new short[1];
			sSizes[0] = 40;
			type2[0] = new AttrType(AttrType.attrString);
			String updateMajor = "Biology";
			Tuple updateTuple = new Tuple();
			updateTuple.setHdr((short)type2.length, type2, sSizes);
			updateTuple.setStrFld(1, updateMajor);
			f.updateColumnofTuple(tid, updateTuple, 2);
			Tuple t5 = f.getTuple(tid);
			System.out.printf("%d, %s, %s, %.2f\n", t5.getIntFld(1), t5.getStrFld(2), t5.getStrFld(3), t5.getFloFld(4));
			
		}catch(Exception e) {
			status = false;
			System.out.println("test failed: " + e.getMessage());
		}
		
		try {
			SystemDefs.JavabaseBM.flushAllPages();
			SystemDefs.JavabaseDB.closeDB();
		}catch(Exception e) {/*empty*/}
		
		
		return status;
	}
	
	
	protected boolean test3()
	{		
		System.out.println ("\n  Test 3: Update a single column in columnar file\n");
		boolean status = OK;
		
		// create database with 100 pages and 100 buffers
		SystemDefs sysdef = new SystemDefs(dbpath,100,100,"Clock");
				
		String fileName = "columnar_file_1"; 
		
		AttrType[] attributes = new AttrType[4];
		attributes[0] = new AttrType(AttrType.attrInteger);
		attributes[1] = new AttrType(AttrType.attrString);
		attributes[2] = new AttrType(AttrType.attrString);
		attributes[3] = new AttrType(AttrType.attrReal);
		
		short[] stringsSizes = new short[2];
		stringsSizes[0] = 40;
		stringsSizes[1] = 40;
		
		Columnarfile f = null;
		//SystemDefs.JavabaseDB.printEntries();
		try {
			f = new Columnarfile(fileName, attributes.length, attributes, stringsSizes);
			
			int id = 100;
			String name = "name";
			String major = "major";
			float credit = 30;
			
			int numRecords = 50;
			TID[] tupleIDs = new TID[numRecords]; 
			//LinkedList<byte[]> records = new LinkedList<>();
			LinkedList<Tuple> tuples = new LinkedList<>();
			
			// create and insert numRecords records
			for(int i = 0; i < numRecords; ++i)
			{
				//records.add(createRecord(id++, name+i, major+i, credit++));
				Tuple t = new Tuple();
				t.setHdr((short)attributes.length, attributes, stringsSizes);
				t.setIntFld(1, id++);
				t.setStrFld(2, name+i);
				t.setStrFld(3, major+i);
				t.setFloFld(4, credit++);
				
				tupleIDs[i] = f.insertTuple(t.getTupleByteArray());
			}
			
			// update the value of some records
			AttrType[] type2 = new AttrType[1];
			short[] sSizes = new short[1];
			sSizes[0] = 40;
			type2[0] = new AttrType(AttrType.attrString);
			String updateName = "Abdu";
			Tuple updateTuple = new Tuple();
			updateTuple.setHdr((short)type2.length, type2, sSizes);
			updateTuple.setStrFld(1, updateName);			
			f.updateColumnofTuple(tupleIDs[numRecords/2], updateTuple, 1);
			
			// print all records
			for(int i = 0; i < numRecords; ++i)
			{
				tuples.add(f.getTuple(tupleIDs[i]));
				System.out.printf("%d, %s, %s, %.2f\n", 
						tuples.get(i).getIntFld(1), 
						tuples.get(i).getStrFld(2), 
						tuples.get(i).getStrFld(3), 
						tuples.get(i).getFloFld(4));
			}
			
		}catch(Exception e) {
			status = false;
			System.out.println("test failed: " + e.getMessage());
		}
		
		try {
			SystemDefs.JavabaseBM.flushAllPages();
			SystemDefs.JavabaseDB.closeDB();
		}catch(Exception e) {/*empty*/}
		
		
		return status;
	}



protected boolean test4()
{		
	System.out.println ("\n  Test 4: TupleScan getNext and position\n");
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
		
		int id = 100;
		String name = "name";
		String major = "major";
		float credit = 30;
		
		int numRecords = 50;
		TID[] tupleIDs = new TID[numRecords]; 
		LinkedList<byte[]> records = new LinkedList<>();
		LinkedList<Tuple> tuples = new LinkedList<>();
		
		// create and insert numRecords records
		for(int i = 0; i < numRecords; ++i)
		{
			records.add(createRecord(id++, name+i, major+i, credit++));
			tupleIDs[i] = f.insertTuple(records.get(i));
		}
		
		// update the value of some records
		AttrType[] type2 = new AttrType[1];
		type2[0] = new AttrType(AttrType.attrString);
		String updateName = "Abdu";
		byte[] data4 = new byte[2 + updateName.length()*2];
		Convert.setShortValue((short) updateName.length(), 0, data4);
		Convert.setStrValue(updateName, 2, data4);
		Tuple t5 = createTuple(data4, type2);			
		f.updateColumnofTuple(tupleIDs[numRecords/2], t5, 1);
		
		// retrieve records using TupleScan object
		System.out.println("Reading tuples using TupleScan");
		TupleScan scanner =  f.openTupleScan();
		Tuple t;
		TID tid = new TID(attributes.length);
		for(t = scanner.getNext(tid); 
				t != null; 
				t = scanner.getNext(tid))
		{
			tuples.add(t);
			System.out.printf("%d, %s, %s, %.2f\n", 
					t.getIntFld(1), 
					t.getStrFld(2), 
					t.getStrFld(3), 
					t.getFloFld(4));
		}
		scanner.closetuplescan();
		
		
		System.out.println("\nTupleScan.position tests\n");
		// test TupleScan.position 1
		System.out.println("calling: scanner.position(tupleIDs[45])");
		status = scanner.position(tupleIDs[45]);
		if(status)
		{
			for(int i = 0; i < 3; ++i) // print 3 records
			{
				t = scanner.getNext(tid);
				System.out.printf("%d, %s, %s, %.2f\n", 
						t.getIntFld(1), 
						t.getStrFld(2), 
						t.getStrFld(3), 
						t.getFloFld(4));
			}
		}
		
		
		// test TupleScan.position 1
		System.out.println("\ncalling: scanner.position(tupleIDs[0])");
		status = scanner.position(tupleIDs[0]);
		if(status)
		{
			for(int i = 0; i < 3; ++i) // print 3 records
			{
				t = scanner.getNext(tid);
				System.out.printf("%d, %s, %s, %.2f\n", 
						t.getIntFld(1), 
						t.getStrFld(2), 
						t.getStrFld(3), 
						t.getFloFld(4));
			}
		}
		
		
	}catch(Exception e) {
		status = FAIL;
		System.out.println("test failed: " + e.getMessage());
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

