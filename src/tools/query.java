package tools;

import heap.*;
import global.*;
import iterator.*;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import diskmgr.DB;

/**
 * The query program performs query on existing row/column-oriented database relations and displays results based on specified conditions.
 *  
 * 
 * @author aalbaltan
 * 
 */
public class query {

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
	
	// Auxiliary function that does string to AttrOperator mapping
	static AttrOperator StringToAttrOperator(String op)
	{
		if(op.equals("="))
			return new AttrOperator(AttrOperator.aopEQ);
		else if(op.equals("!="))
			return new AttrOperator(AttrOperator.aopNE);
		else if(op.equals("<"))
			return new AttrOperator(AttrOperator.aopLT);
		else if(op.equals("<="))
			return new AttrOperator(AttrOperator.aopLE);
		else if(op.equals(">"))
			return new AttrOperator(AttrOperator.aopGT);
		else if(op.equals(">="))
			return new AttrOperator(AttrOperator.aopGE);
		
		return null;
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
	
	public static void main(String[] args) {
		final String usage = "\tUsage: query DBNAME TABLENAME VALUECONSTRAINT NUMBUF\n";
		
		// validate arguments
		if(args.length != 6) {
			System.out.println(usage);
			System.exit(1);
		}
		
		String databaseName = args[0];
		String tableName = args[1];
		
		String columnName = args[2];
		String operator = args[3];
		String value = args[4];
		
		int num_buffers = 0;
		try {
			num_buffers = Integer.parseInt(args[5]); 			// memory buffer pool size
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
			CondExpr[] outFilter = new CondExpr[2];
			outFilter[0] = new CondExpr();
			outFilter[0].op    = StringToAttrOperator(operator);
			outFilter[0].next  = null;
			outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
			outFilter[0].type2 = hdr.types[columnIndex];
			outFilter[0].operand1.symbol = hdr.select[columnIndex];
		
			if(hdr.types[columnIndex].attrType == AttrType.attrInteger)
				outFilter[0].operand2.integer = Integer.parseInt(value);
			else if(hdr.types[columnIndex].attrType == AttrType.attrString)
				outFilter[0].operand2.string = value;
			else if(hdr.types[columnIndex].attrType == AttrType.attrReal)
				outFilter[0].operand2.real = Float.parseFloat(value);
			
			// profiling
			long start = System.nanoTime();
			
			Iterator scanner = null;
			try {
				if (!hdr.columnar)
					scanner = new FileScan(hdr.tableName, hdr.types, hdr.strSizes, (short) hdr.length,
							(short) hdr.length, hdr.select, outFilter);
				else
					scanner = new ColumnarFileScan(hdr.tableName, hdr.types, hdr.strSizes, hdr.select, outFilter);
			

				Tuple t = scanner.get_next();
				if (t == null) {
					System.out.println("no records match specified condition");
					return;
				}
				
				// print table
				System.out.println(String.join(" ", hdr.columns));
				while( t != null)
				{
					t.print(hdr.types);
			    	t = scanner.get_next();
				}

			} finally {
				if (scanner != null)
					scanner.close();
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
