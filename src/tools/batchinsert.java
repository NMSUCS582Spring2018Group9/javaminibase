package tools;

import global.*;
import heap.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import columnar.Columnarfile;

/**
 * The batchinsert program inserts a given file into the database. The inserted data can either be stored as rows or columns
 *  
 * 
 * @author aalbaltan
 * 
 */
public class batchinsert {

	// Auxiliary function that translate a given string into AttrType type
	static AttrType StringToAttrType(String str)
	{
		str = str.toLowerCase();
		if(str.equals("int"))
			return new AttrType(AttrType.attrInteger);
		else if(str.equals("float"))
			return new AttrType(AttrType.attrReal);
		else
			return new AttrType(AttrType.attrString);
	}
	
	public static void main(String[] args) {
		final String usage = "\tUsage: batchinsert DATAFILENAME DBNAME DBTYPE TABLENAME NUMCOLUMNS\n";
		
		// validate arguments
		if(args.length != 5) {
			System.out.println(usage);
			System.exit(1);
		}
		
		final int STRING_COLUMN_SIZE = 30;		// fixed-size string fields
		final int NUM_PAGES = 1024; 			// DB size = 1 MB
		final int NUM_BUFFERS = 100; 			// memory buffer pool size
		String datafile_name = args[0];
		String db_name = args[1];
		String db_type = args[2];
		String tableName = args[3];
		String tableHeader = new String();
		int num_columns = Integer.parseInt(args[4]);
		int numStringColumns = 0;
		
		AttrType[] columnsTypes = new AttrType[num_columns];
		String[] columnsNames = new String[num_columns];
		String pairs_delims = " ";
		String types_delims = ":";
		
		// validate arguments
		if(!db_type.equals("row") && !db_type.equals("column"))
		{
			System.out.println("invalid db_type. Allowed types: row, column");
			System.exit(1);
		}
		
		SystemDefs sysdef = null;
		// check whether the db already exists
		File db_file = new File(db_name);
		if(db_file.exists()) {	// file found
			System.out.printf("An existing database (%s) was found, opening database.\n", db_name);
			// open database with 100 buffers
			sysdef = new SystemDefs(db_name,0,NUM_BUFFERS,"Clock");
		}else
		{
			System.out.printf("Creating a new database (%s).\n", db_name);
			// create database with 100 pages and 100 buffers
			sysdef = new SystemDefs(db_name,NUM_PAGES,NUM_BUFFERS,"Clock");
		}
		
		File f = new File(datafile_name);
		if(f.exists() && !f.isDirectory())	// file found
		{
			try {
				FileReader fReader = new FileReader(f);
				BufferedReader bReader = new BufferedReader(fReader);
				String line;
				
				// read data header and parse columns types
				line = bReader.readLine();
				tableHeader = line;
				String[] pairs = line.split(pairs_delims);
				for(int i = 0; i < num_columns; ++i)
				{
					String[] name_type = pairs[i].split(types_delims);
					columnsNames[i] = name_type[0];
					columnsTypes[i] = StringToAttrType(name_type[1]);
				}
				
				// count number of string columns
				for(int i = 0; i < columnsTypes.length; ++i)
					if(columnsTypes[i].attrType == AttrType.attrString)
						++numStringColumns;
				
				//TODO(aalbaltan) TEST CASE: try passing an input file that contains no string columns
				short[] stringsSizes = null;
				if(numStringColumns > 0) {
					stringsSizes = new short[numStringColumns];
					for(int i = 0; i < stringsSizes.length; ++i)
						stringsSizes[i] = STRING_COLUMN_SIZE;
				}
				
				if(db_type.equals("row")) // row-oriented relation 
				{
					tableName += "_row";
					Heapfile heapFile = new Heapfile(tableName);
					System.out.printf("Inserting records into %s.\n", tableName);
					
					int recordsInserted = 0;
					long start = System.nanoTime();
					// read actual data line by line
					while((line = bReader.readLine()) != null) {
						String[] columnsData = line.split(" ");
						Tuple t = new Tuple();
						t.setHdr((short)num_columns, columnsTypes, stringsSizes);
						
						for(int i = 0; i < columnsData.length; ++i)
						{
							switch(columnsTypes[i].attrType)
							{
							case AttrType.attrInteger:
								t.setIntFld(i+1, Integer.parseInt(columnsData[i]));
								break;
							case AttrType.attrString:
								t.setStrFld(i+1, columnsData[i]);
								break;
							case AttrType.attrReal:
								t.setFloFld(i+1, Float.parseFloat(columnsData[i]));
								break;
							}
						}
						heapFile.insertRecord(t.getTupleByteArray());
						++recordsInserted;
					}
					long end = System.nanoTime();
					long duration = (end - start)/1000000;	// in milliseconds
					
					System.out.printf("%d records inserted. Table %s has %d records.\n"
							+ "elapsed time: %d ms\n", recordsInserted, tableName, heapFile.getRecCnt(), duration);
				}
				else if(db_type.equals("column")) // column-oriented relation 
				{
					tableName += "_column";
					Columnarfile columnarFile = new Columnarfile(tableName, num_columns, columnsTypes, stringsSizes);
					System.out.printf("Inserting records into %s.\n", tableName);
					
					int recordsInserted = 0;
					long start = System.nanoTime();
					
					// read actual data line by line
					while((line = bReader.readLine()) != null) {
						String[] columnsData = line.split(" ");
						Tuple t = new Tuple();
						t.setHdr((short)num_columns, columnsTypes, stringsSizes);
						
						for(int i = 0; i < columnsData.length; ++i)
						{
							switch(columnsTypes[i].attrType)
							{
							case AttrType.attrInteger:
								t.setIntFld(i+1, Integer.parseInt(columnsData[i]));
								break;
							case AttrType.attrString:
								t.setStrFld(i+1, columnsData[i]);
								break;
							case AttrType.attrReal:
								t.setFloFld(i+1, Float.parseFloat(columnsData[i]));
								break;
							}
						}
						columnarFile.insertTuple(t.getTupleByteArray());
						++recordsInserted;
					}
					long end = System.nanoTime();
					long duration = (end - start)/1000000;	// in milliseconds
					
					System.out.printf("%d records inserted. Table %s has %d records.\n"
							+ "elapsed time: %d ms\n", recordsInserted, tableName, columnarFile.getTupleCnt(), duration);
				}
				
				fReader.close();
				//System.out.println(sBuffer.toString());
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (HFException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (HFBufMgrException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (HFDiskMgrException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidSlotNumberException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTupleSizeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SpaceNotAvailableException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (FieldNumberOutOfBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTypeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try {
			sysdef.JavabaseBM.flushAllPages();
			sysdef.JavabaseDB.closeDB();
		}catch(Exception e) {/*empty*/}
		
		// print page I/O info
		System.out.printf("total page reads: %d\ttotal page writes: %d\n", 
				sysdef.JavabaseDB.GetNumberOfPageReads(),
				sysdef.JavabaseDB.GetNumberOfPageWrites());	
		
		try {
			// store header in another table. This is needed by query to identify the columns of the table since
			// javaminibase base does not have a catalog (catalog is disabled)
			sysdef = new SystemDefs(db_name,0,NUM_BUFFERS,"Clock");
			Heapfile typeFile = new Heapfile(tableName+"_type");
			int recordsCount = typeFile.getRecCnt();
			if(recordsCount == 0) {
				Tuple headerTuple = new Tuple();
				AttrType[] headerTypes = new AttrType[1];
				headerTypes[0] = new AttrType(AttrType.attrString);
				short[] headerStringSizes = new short[1];
				headerStringSizes[0] = 900;
				headerTuple.setHdr((short)1, headerTypes, headerStringSizes);
				headerTuple.setStrFld(1, tableHeader);
				typeFile.insertRecord(headerTuple.getTupleByteArray());
				sysdef.JavabaseBM.flushAllPages();
				sysdef.JavabaseDB.closeDB();
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}

