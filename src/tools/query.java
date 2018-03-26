package tools;

import heap.*;
import global.*;
import iterator.*;
import java.io.File;

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
		final String usage = "\tUsage: query DBNAME VALUECONSTRAINT NUMBUF\n";
		
		// validate arguments
		if(args.length != 5) {
			System.out.println(usage);
			System.exit(1);
		}
		
		final int STRING_COLUMN_SIZE = 30;				// fixed-size string fields
		String rowTableName = "table_1_row";
		String columnTableName = "table_1_column";
		String headerTableName = new String();
		String headerString = new String();
		String db_name = args[0];
		String column_name = args[1];
		String operator = args[2];
		String value = args[3];
		int num_buffers = 0;
		try {
			num_buffers = Integer.parseInt(args[4]); 			// memory buffer pool size
		} catch(NumberFormatException e) {
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
			
			// preparing query condition
			//NOTE: CondExpr array has to contain an extra (null) element otherwise PredEval.Eval would throw an exception 
			int columnIndex = GetAttrType(columnsNames, column_name);
			CondExpr[] outFilter = new CondExpr[2];
			outFilter[0] = new CondExpr();
			outFilter[0].op    = StringToAttrOperator(operator);
			outFilter[0].next  = null;
			outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
			outFilter[0].type2 = columnsTypes[columnIndex];
			outFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),columnIndex+1);
			//outFilter[0].operand2.integer = 990;
			if(columnsTypes[columnIndex].attrType == AttrType.attrInteger)
				outFilter[0].operand2.integer = Integer.parseInt(value);
			else if(columnsTypes[columnIndex].attrType == AttrType.attrString)
				outFilter[0].operand2.string = value;
			else if(columnsTypes[columnIndex].attrType == AttrType.attrReal)
				outFilter[0].operand2.real = Float.parseFloat(value);
			
			// projection list
			FldSpec [] Sprojection = new FldSpec[columnsTypes.length];
			for(int i = 0; i < columnsTypes.length; ++i)
			    Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
			
			// profiling
			long start = System.nanoTime();
			
			// row query
			if(rowPage != null)
			{
				FileScan fileScan = new FileScan(
						rowTableName, 
						columnsTypes,
						stringsSizes,
						(short) columnsTypes.length, 
						(short) columnsTypes.length,
						Sprojection, 
						outFilter);
				
				// read and print all tuples in record set
				Tuple t = fileScan.get_next();
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
			    	t = fileScan.get_next();
				}
				fileScan.close();
				System.out.println();
			}
			// col query
			else if(columnPage != null)
			{
				ColumnarFileScan columnarFileScan = new ColumnarFileScan(
						columnTableName, 
						columnsTypes, 
						stringsSizes, 
						Sprojection, 
						outFilter);
				
				// read and print all tuples in record set
				Tuple t = columnarFileScan.get_next();
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
			    	t = columnarFileScan.get_next();
				}
				columnarFileScan.close();
				System.out.println();
			}
			
			long end = System.nanoTime();
			long duration = (end - start)/1000000;	// in milliseconds
			
			System.out.printf("elapsed time: %d ms\n", duration);
			
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			sysdef.JavabaseBM.flushAllPages();
			sysdef.JavabaseDB.closeDB();
		}catch(Exception e) {/*empty*/}
		
		// print page I/O info
		System.out.printf("total page reads: %d\ttotal page writes: %d\n", 
				sysdef.JavabaseDB.GetNumberOfPageReads(),
				sysdef.JavabaseDB.GetNumberOfPageWrites());
	}

}
