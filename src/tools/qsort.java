package tools;

import heap.*;
import global.*;
import iterator.*;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import diskmgr.DB;

/**
 * The qsort program accesses a database table and sorts the results based on a specified column
 *  
 * 
 * @author Shane
 * 
 */
public class qsort {
	public static void main(String[] args) {
		final String usage = "\tUsage: qsort DBNAME TABLENAME COLUMNNAME NUMBUF\n";
		
		// validate arguments
		if(args.length != 4) {
			System.out.println(usage);
			System.exit(1);
		}
		
		String databaseName = args[0];
		String tableName = args[1];
		String columnName = args[2];
		int num_buffers = 0;
		try {
			num_buffers = Integer.parseInt(args[3]); 			// memory buffer pool size
		} catch(NumberFormatException e) {
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

			
			long end = System.nanoTime();
			long duration = (end - start)/1000000;	// in milliseconds
			
			System.out.printf("elapsed time: %d ms\n", duration);
			
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			SystemDefs.JavabaseBM.flushAllPages();
			SystemDefs.JavabaseDB.closeDB();
		}catch(Exception e) {/*empty*/}
		
		// print page I/O info
		System.out.println(String.format("total page reads: %d", DB.GetNumberOfPageReads()));
		System.out.println(String.format("total page writes: %d\n", DB.GetNumberOfPageWrites()));
	}

}
