package tools;

import heap.*;
import global.*;
import iterator.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import diskmgr.DB;

/**
 * The qNLJ program performs a nested loop join on two relations
 * 
 * 
 * @author shane
 * 
 */
public class qNLJ {

	static final String usage = "\tUsage: qNLJ DBNAME TABLENAME1 COLUMNNAME1 TABLENAME2 COLUMNNAME2 NUMBUF";

	public static void main(String[] args) throws HashOperationException, PageUnpinnedException, PagePinnedException,
			PageNotFoundException, BufMgrException, IOException {

		// validate arguments
		if (args.length != 6) {
			System.out.println(usage);
			System.exit(1);
		}

		String databaseName = args[0];

		String tableNameA = args[1];
		String columnNameA = args[2];
		String tableNameB = args[3];
		String columnNameB = args[4];

		int num_buffers = 0;
		try {
			num_buffers = Integer.parseInt(args[5]); // memory buffer pool size
		} catch (NumberFormatException e) {
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
			// gather header information
			Header hdrA = new Header(tableNameA, false);
			Header hdrB = new Header(tableNameB, true);
			
			System.out.println(hdrA.toString());
			System.out.println(hdrB.toString());

			// profiling
			long start = System.nanoTime();

			// Nested Loop Join
			Iterator scannerA = null;
			Iterator nlj = null;
			String[] columns;
			FldSpec[] select;
			AttrType[] types;
			int numPages = 10;
			try {
				if (!hdrA.columnar)
					scannerA = new FileScan(hdrA.tableName, hdrA.types, hdrA.strSizes, (short) hdrA.types.length,
							(short) hdrA.types.length, hdrA.select, null);
				else
					scannerA = new ColumnarFileScan(hdrA.tableName, hdrA.types, hdrA.strSizes, hdrA.select, null);

				columns = concat(hdrA.columns, hdrB.columns);
				select = concat(hdrA.select, hdrB.select);
				types = concat(hdrA.types, hdrB.types);
				
				CondExpr[] join = new CondExpr[2];
				join[0] = new CondExpr();
				join[0].next  = null;
				join[0].op    = new AttrOperator(AttrOperator.aopEQ); // =
				join[0].type1 = new AttrType(AttrType.attrSymbol);
				join[0].type2 = new AttrType(AttrType.attrSymbol);
				join[0].operand1.symbol = hdrA.select[hdrA.indexOf(columnNameA)];
				join[0].operand2.symbol = hdrB.select[hdrB.indexOf(columnNameB)];
				join[1] = null;
				
				nlj = new NestedLoopsJoins(hdrA.types, hdrA.types.length, hdrA.strSizes, 
										   hdrB.types, hdrB.types.length, hdrB.strSizes,
						                   numPages, scannerA, hdrB.tableName, 
						                   join, null, select, select.length);

				Tuple t = nlj.get_next();
				if (t == null) {
					System.out.println("no records match specified condition");
					System.exit(0);
				}

				// print header
				System.out.println(String.join(" ", columns));

				// print data
				while (t != null) {
					t.print(types);
					t = nlj.get_next();
				}

			} finally {
				if (scannerA != null)
					scannerA.close();
				if (nlj != null)
					nlj.close();
			}

			long end = System.nanoTime();
			long duration = (end - start) / 1000000; // in milliseconds

			System.out.printf("elapsed time: %d ms\n", duration);

		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			SystemDefs.JavabaseBM.flushAllPages();
			SystemDefs.JavabaseDB.closeDB();
		}

		// print page I/O info
		System.out.println(String.format("total page reads: %d", DB.GetNumberOfPageReads()));
		System.out.println(String.format("total page writes: %d\n", DB.GetNumberOfPageWrites()));
	}
	
	public static <T> T[] concat(T[] first, T[] second) {
		  T[] result = Arrays.copyOf(first, first.length + second.length);
		  System.arraycopy(second, 0, result, first.length, second.length);
		  return result;
		}
}
