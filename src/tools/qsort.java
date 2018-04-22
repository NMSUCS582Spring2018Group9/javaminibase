package tools;

import heap.*;
import global.*;
import iterator.*;
import java.io.File;
<<<<<<< HEAD
import java.nio.file.Files;
import java.nio.file.Paths;

import diskmgr.DB;
=======
>>>>>>> 0ab336d3812d83c91dca424c4cd9be9e14068591

/**
 * The qsort program accesses a database table and sorts the results based on a specified column
 *  
 * 
 * @author Shane
 * 
 */
public class qsort {
<<<<<<< HEAD
=======

	// Auxiliary function that does string to AttrType mapping
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
	
	// Auxiliary function to get the index associated with column's name
	static int GetAttrType(String[] columnNames, String column)
	{
		for(int i = 0; i < columnNames.length; ++i)
		{
			if(columnNames[i].equals(column))
				return i;
		}
		return -1;
	}
	
>>>>>>> 0ab336d3812d83c91dca424c4cd9be9e14068591
	public static void main(String[] args) {
		final String usage = "\tUsage: qsort DBNAME TABLENAME COLUMNNAME NUMBUF\n";
		
		// validate arguments
		if(args.length != 4) {
			System.out.println(usage);
			System.exit(1);
		}
		
<<<<<<< HEAD
		String databaseName = args[0];
		String tableName = args[1];
		String columnName = args[2];
=======
		final int STRING_COLUMN_SIZE = 30;				// fixed-size string fields
		final int buffer_pages = 8;
		String headerTableName = new String();
		String headerString = new String();
		String db_name = args[0];
		String table_name = args[1];
		String rowTableName = table_name + "_row";
		String columnTableName = table_name + "_column";
		String column_name = args[2];
>>>>>>> 0ab336d3812d83c91dca424c4cd9be9e14068591
		int num_buffers = 0;
		try {
			num_buffers = Integer.parseInt(args[3]); 			// memory buffer pool size
		} catch(NumberFormatException e) {
<<<<<<< HEAD
			System.out.println("invalid NUMBUF input");
			System.out.println(usage);
			System.exit(1);
		}
		
		
		// check whether or not the database already exists
		if (Files.notExists(Paths.get(databaseName))) {
			System.out.println("Database '" + databaseName + "' not found, exiting.");
			System.out.println(usage);
			System.exit(1);
		}

		// open database
		System.out.println("Database '" + databaseName + "' found, opening with " + num_buffers + " buffers.\n");
		new SystemDefs(databaseName, 0, num_buffers, "Clock");
		
		
		try {
			Header hdr = new Header(tableName, false);	
			
			// preparing query condition 
			//NOTE: CondExpr array has to contain an extra (null) element otherwise PredEval.Eval would throw an exception 
			int columnIndex = hdr.indexOf(columnName);
			
			// profiling
			long start = System.nanoTime();

			Iterator scanner = null;
			Sort sorter = null;
			try {
				if (!hdr.columnar)
					scanner = new FileScan(hdr.tableName, hdr.types, hdr.strSizes, (short) hdr.length,
							(short) hdr.length, hdr.select, null);
				else
					scanner = new ColumnarFileScan(hdr.tableName, hdr.types, hdr.strSizes, hdr.select, null);
			
				sorter = new Sort(hdr.types, (short) hdr.length, hdr.strSizes, scanner, columnIndex + 1,
						new TupleOrder(TupleOrder.Ascending), hdr.fldSizes[columnIndex], 30);
				
				Tuple t = sorter.get_next();
				if (t == null) {
					System.out.println("no records match specified condition");
					return;
				}
				
				// print table
				System.out.println(String.join(" ", hdr.columns));
				while( t != null)
				{
					t.print(hdr.types);
			    	t = sorter.get_next();
				}
			} finally {
				if (scanner != null)
					scanner.close();
				if (sorter != null)
					sorter.close();
			}

=======
			System.out.println("invalid NUMBUF input\n" + usage);
			System.exit(1);
		}
		
		int numStringColumns = 0;
		
		AttrType[] columnsTypes;
		String[] columnsNames;
		String pairs_delims = " ";
		String types_delims = ":";
		
		SystemDefs sysdef = null;
		// check whether the db already exists
		File db_file = new File(db_name);
		if(db_file.exists()) {	// file found
			System.out.printf("An existing database (%s) was found, opening database with %d buffers.\n", db_name, num_buffers);
			// open database with 100 buffers
			sysdef = new SystemDefs(db_name,0,num_buffers,"Clock");
		}else
		{
			System.out.println("Database not found, exiting.\n" + usage);
			System.exit(1);
		}
		
		try {
			PageId rowPage = sysdef.JavabaseDB.get_file_entry(rowTableName);
			PageId columnPage = sysdef.JavabaseDB.get_file_entry(columnTableName+".hdr");
			
			// check table type (row or column)
			if(rowPage != null)
			{				
				headerTableName = rowTableName + "_type";
			}
			else if(columnPage != null)
			{
				headerTableName = columnTableName + "_type";
			}
			
			// read table's header info
			Heapfile headerFile = new Heapfile(headerTableName);
			Tuple headerTuple = new Tuple();
			short[] sSizes = new short[1];
			sSizes[0] = 900;
			AttrType[] types = new AttrType[1];
			types[0] = new AttrType(AttrType.attrString);
			RID rid = new RID();
			Scan s = headerFile.openScan();
			headerTuple = s.getNext(rid);
			if(headerTuple == null) {
				System.out.println("Attributes types table not found.\n");
				System.exit(1);
			}
			headerTuple.setHdr((short)1, types, sSizes);
			headerString = headerTuple.getStrFld(1);
			
			String[] pairs = headerString.split(pairs_delims);
			columnsNames = new String[pairs.length];
			columnsTypes = new AttrType[pairs.length];
			for(int i = 0; i < pairs.length; ++i)
			{
				String[] name_type = pairs[i].split(types_delims);
				columnsNames[i] = name_type[0].toLowerCase();
				columnsTypes[i] = StringToAttrType(name_type[1]);
			}
			
			// count number of string columns
			for(int i = 0; i < columnsTypes.length; ++i)
				if(columnsTypes[i].attrType == AttrType.attrString)
					++numStringColumns;
			
			short[] stringsSizes = null;
			if(numStringColumns > 0) {
				stringsSizes = new short[numStringColumns];
				for(int i = 0; i < stringsSizes.length; ++i)
					stringsSizes[i] = STRING_COLUMN_SIZE;
			}
			
			// preparing query condition TODO: change to sorting condition 
			//NOTE: CondExpr array has to contain an extra (null) element otherwise PredEval.Eval would throw an exception 
			int columnIndex = GetAttrType(columnsNames, column_name);
			int sortFldLen;
			switch (columnsTypes[columnIndex].attrType)
			{
			case AttrType.attrInteger:
			  sortFldLen = 4;
			  break;
			case AttrType.attrReal:
			  sortFldLen = 4;
			  break;
			case AttrType.attrString:
			  sortFldLen = STRING_COLUMN_SIZE;
			  break;
			default:
			  //error("Unknown type");
			  return;
			}
			
			// projection list
			FldSpec [] Sprojection = new FldSpec[columnsTypes.length];
			for(int i = 0; i < columnsTypes.length; ++i)
			    Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
			
			// profiling
			long start = System.nanoTime();
			
			// row query TODO: add sorting on row
			if(rowPage != null)
			{
				FileScan fileScan = null;
				Sort sorter = null;
				try { 
					fileScan = new FileScan(
						rowTableName, 
						columnsTypes,
						stringsSizes,
						(short) columnsTypes.length, 
						(short) columnsTypes.length,
						Sprojection, 
						null);
		
					sorter = new Sort(columnsTypes, 
						(short) columnsTypes.length, 
						stringsSizes, 
						fileScan, 
						columnIndex + 1, 
						new TupleOrder(TupleOrder.Ascending), 
						sortFldLen, buffer_pages);
					
					// read and print all tuples in record set
					Tuple t = sorter.get_next();
					if(t!= null) {
						for(int i = 0; i < columnsTypes.length; ++i)
							System.out.printf("%s ", columnsNames[i]);
						System.out.println();
					}
					else
						System.out.println("no records match specified condition");
					
					while( t != null)
					{
						for(int i = 0; i < columnsTypes.length; ++i)
						{
							if(columnsTypes[i].attrType == AttrType.attrInteger)
								System.out.printf("%d ", t.getIntFld(i+1));
							else if(columnsTypes[i].attrType == AttrType.attrString)
								System.out.printf("%s ", t.getStrFld(i+1));
							else if(columnsTypes[i].attrType == AttrType.attrReal)
								System.out.printf("%.2f ", t.getFloFld(i+1));
						}
						System.out.println();
				    	t = sorter.get_next();
					}
				}
				finally {
					if (fileScan != null)
						fileScan.close();
					if (sorter != null)
						sorter.close();
				}
				System.out.println();
			}
			// col query TODO: add sorting on column
			else if(columnPage != null)
			{
				ColumnarFileScan columnarFileScan = null;
				Sort sorter = null;
				try { 
					columnarFileScan = new ColumnarFileScan(
							columnTableName, 
							columnsTypes, 
							stringsSizes, 
							Sprojection, 
							null);
	
					sorter = new Sort(columnsTypes, 
							(short) columnsTypes.length, 
							stringsSizes, 
							columnarFileScan, 
							columnIndex + 1, 
							new TupleOrder(TupleOrder.Ascending), 
							sortFldLen, buffer_pages);
					
					// read and print all tuples in record set
					Tuple t = sorter.get_next();
					if(t!= null) {
						for(int i = 0; i < columnsTypes.length; ++i)
							System.out.printf("%s ", columnsNames[i]);
						System.out.println();
					}
					else
						System.out.println("no records match specified condition");
					
					while( t != null)
					{
						for(int i = 0; i < columnsTypes.length; ++i)
						{
							if(columnsTypes[i].attrType == AttrType.attrInteger)
								System.out.printf("%d ", t.getIntFld(i+1));
							else if(columnsTypes[i].attrType == AttrType.attrString)
								System.out.printf("%s ", t.getStrFld(i+1));
							else if(columnsTypes[i].attrType == AttrType.attrReal)
								System.out.printf("%.2f ", t.getFloFld(i+1));
						}
						System.out.println();
				    	t = sorter.get_next();
					}
				}
				finally {
					if (columnarFileScan != null)
						columnarFileScan.close();
					if (sorter != null)
						sorter.close();
				}
				System.out.println();
			}
>>>>>>> 0ab336d3812d83c91dca424c4cd9be9e14068591
			
			long end = System.nanoTime();
			long duration = (end - start)/1000000;	// in milliseconds
			
			System.out.printf("elapsed time: %d ms\n", duration);
			
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
<<<<<<< HEAD
			SystemDefs.JavabaseBM.flushAllPages();
			SystemDefs.JavabaseDB.closeDB();
		}catch(Exception e) {/*empty*/}
		
		// print page I/O info
		System.out.println(String.format("total page reads: %d", DB.GetNumberOfPageReads()));
		System.out.println(String.format("total page writes: %d\n", DB.GetNumberOfPageWrites()));
=======
			sysdef.JavabaseBM.flushAllPages();
			sysdef.JavabaseDB.closeDB();
		}catch(Exception e) {/*empty*/}
		
		// print page I/O info
		System.out.printf("total page reads: %d\ttotal page writes: %d\n", 
				sysdef.JavabaseDB.GetNumberOfPageReads(),
				sysdef.JavabaseDB.GetNumberOfPageWrites());
>>>>>>> 0ab336d3812d83c91dca424c4cd9be9e14068591
	}

}
