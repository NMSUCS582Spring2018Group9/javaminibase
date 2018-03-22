package tests;

import java.io.*;
import java.util.*;

import com.sun.xml.internal.bind.v2.schemagen.xmlschema.AttributeType;

import java.lang.*;
//import heap.*;
//import bufmgr.*;
//import diskmgr.*;
import global.*;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
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
	
	Tuple createTuple(byte[] array, AttrType[] types) throws IOException, InvalidTypeException, InvalidTupleSizeException 
	{
		short []strings_sizes = new short[types.length];
		byte[] buff = new byte[1024];
		int read_pos = 0;
		int pos = 4 + types.length * 2;
		int numStrings = 0;
		
		for(int i = 0; i < types.length; ++i)
		{
			switch(types[i].attrType)
			{
			case AttrType.attrInteger:
				int int_val = Convert.getIntValue(read_pos, array);
				read_pos += 4;
				Convert.setIntValue(int_val, pos, buff);
				pos += 4;
				break;
			case AttrType.attrString:
				short str_len = Convert.getShortValue(read_pos, array);
				read_pos += 2;
				strings_sizes[numStrings] = (short)(str_len*2);
				++numStrings;
				String str = Convert.getStrValue(read_pos, array, str_len*2);
				Convert.setStrValue(str, pos, buff);
				pos += str_len * 2 + 2;
				read_pos += str_len * 2;				
				break;
			case AttrType.attrReal:
				float float_val = Convert.getFloValue(read_pos, array);
				read_pos += 4;
				Convert.setFloValue(float_val, pos, buff);
				pos += 4;
				break;
			//TODO(aalbaltan) handle other cases
			default:
				break;
			}
		}
		
		short[] stringSizes2 = new short[numStrings];
		System.arraycopy(strings_sizes, 0, stringSizes2, 0, numStrings);
		
		byte[] buff2 = new byte[pos];
		System.arraycopy(buff, 0, buff2, 0, buff2.length);
		Tuple atuple = new Tuple(buff2, 0, buff2.length);
		atuple.setHdr((short)types.length, types, stringSizes2);
		
		return atuple;
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
		
		Columnarfile f = null;
		//SystemDefs.JavabaseDB.printEntries();
		try {
			f = new Columnarfile(fileName, attributes.length, attributes);
			
			if(f.getTupleCnt()!= 0)
				throw new Exception("file's tuples count: expected: 0, actual: " + f.getTupleCnt());
			
			int id = 100;
			String name = "Abdu";
			String major = "CS Unfortunately";
			float credit = 39;
			
			byte[] data = createRecord(id, name, major, credit);
			
			
			TID tid = f.insertTuple(data);
			
			if(f.getTupleCnt()!= 1)
				throw new Exception("file's tuples count: expected: 1, actual: " + f.getTupleCnt());
			
			// prepare another record for insertion
			int id2 = 99;
			String name2 = "James";
			String major2 = "Engineering";
			float credit2 = 14;
			
			byte[] data2 = createRecord(id2, name2, major2, credit2);
			
			TID tid2 = f.insertTuple(data2);
			
			if(f.getTupleCnt()!= 2)
				throw new Exception("file's tuples count: expected: 2, actual: " + f.getTupleCnt());
			
			// read the inserted tuple back
			Tuple t = f.getTuple(tid);
			if(t.getIntFld(1) != id)
				status = false;
			if(!t.getStrFld(2).equals(name))
				status = false;
			if(!t.getStrFld(3).equals(major))
				status = false;
			if(Float.compare(t.getFloFld(4), credit) != 0)
				status = false;
			
			Tuple t2 = f.getTuple(tid2);
			if(t2.getIntFld(1) != id2)
				status = false;
			if(!t2.getStrFld(2).equals(name2))
				status = false;
			if(!t2.getStrFld(3).equals(major2))
				status = false;
			if(Float.compare(t2.getFloFld(4), credit2) != 0)
				status = false;
			
			TID fakeTID = new TID(4);
			if(f.updateTuple(fakeTID, t))
				status = false;
			
			
			id = 100;
			name = "Abdu";
			major = "CS";
			credit = 40;
			
			byte[] data3 = createRecord(id, name, major, credit);
			f.updateTuple(tid, createTuple(data3, attributes));
			
			Tuple t3 = f.getTuple(tid);
			Tuple t4 = f.getTuple(tid2);
			System.out.printf("%d, %s, %s, %.2f\n", t3.getIntFld(1), t3.getStrFld(2), t3.getStrFld(3), t3.getFloFld(4));
			System.out.printf("%d, %s, %s, %.2f\n", t4.getIntFld(1), t4.getStrFld(2), t4.getStrFld(3), t4.getFloFld(4));
			
			if(f.getTupleCnt() != 2)
				status = false;
			
			// update a single column
			AttrType[] type2 = new AttrType[1];
			type2[0] = new AttrType(AttrType.attrString);
			String updateMajor = "Biology";
			byte[] data4 = new byte[2 + updateMajor.length()*2];
			Convert.setShortValue((short) updateMajor.length(), 0, data4);
			Convert.setStrValue(updateMajor, 2, data4);
			Tuple t5 = createTuple(data4, type2);
			f.updateColumnofTuple(tid, t5, 2);
			t = f.getTuple(tid);
			System.out.printf("%d, %s, %s, %.2f\n", t.getIntFld(1), t.getStrFld(2), t.getStrFld(3), t.getFloFld(4));
			
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

