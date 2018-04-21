package tools;

import java.util.Arrays;

import global.AttrType;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import iterator.FldSpec;
import iterator.RelSpec;

public class Header {
	public final String tableName;
	public final int length;
	public final String[] columns;
	public final AttrType[] types;
	public final FldSpec[] select;
	public final short[] strSizes;
	public final short[] fldSizes;
	public final boolean columnar;

	final static int STRING_COLUMN_SIZE = 30; // fixed-size string fields

	public Header(String table, boolean inner) throws Exception {
		// check if header or column version exists
		PageId pageRow = SystemDefs.JavabaseDB.get_file_entry(table + "_row");
		PageId pageCol = SystemDefs.JavabaseDB.get_file_entry(table + "_column.hdr");
		if (pageRow != null) // not a row stored tabled
		{
			tableName = table + "_row";
			columnar = false;

		} else if (pageCol != null) {
			tableName = table + "_column";
			columnar = true;
		} else {
			throw new Exception("Attribute Table not found");
		}

		// get header info
		Scan hScan = null;
		try {
			// header data is stored in a single tuple in the header file.
			Heapfile headerFile = new Heapfile(tableName + "_type");
			hScan = headerFile.openScan();
			Tuple t = hScan.getNext(new RID());
			if (t == null) {
				System.out.println("Attributes types table " + tableName + "_type not found.\n");
				System.exit(1);
			}
			t.setHdr((short) 1, new AttrType[] { new AttrType(AttrType.attrString) }, new short[] { 900 });
			String[] columnPairs = t.getStrFld(1).split(" ");
			length = columnPairs.length;
			columns = new String[length];
			fldSizes = new short[length];
			types = new AttrType[length];
			int numStrCols = 0;
			for (int i = 0; i < length; i++) {
				String[] pair = columnPairs[i].split(":");
				columns[i] = pair[0].toLowerCase();
				switch (pair[1].toLowerCase()) {
				case "int":
					types[i] = new AttrType(AttrType.attrInteger);
					fldSizes[i] = 4;
					break;
				case "float":
					types[i] = new AttrType(AttrType.attrReal);
					fldSizes[i] = 4;
					break;
				default:
					types[i] = new AttrType(AttrType.attrString);
					fldSizes[i] = STRING_COLUMN_SIZE;
					numStrCols++;
					break;
				}
			}

			select = new FldSpec[length];
			for (int i = 0; i < length; ++i)
				select[i] = new FldSpec(new RelSpec(inner ? RelSpec.innerRel : RelSpec.outer), i + 1);
			
			strSizes = new short[numStrCols];
			for (int i = 0; i < numStrCols; i++)
				strSizes[i] = STRING_COLUMN_SIZE;

		} finally {
			// close scanner
			if (hScan != null)
				hScan.closescan();
		}
	}
	
	public int indexOf(String column) {
		for (int i = 0; i < columns.length; i++)
		{
			if (columns[i].equals(column))
				return i;
		}
		return -1;
	}
	
	public @Override String toString() {
		String[] types_arr = Arrays.stream(types).map((x) -> x.toString()).toArray(String[]::new);
		String[] specs_arr = Arrays.stream(select).map((x) -> x.offset + "," + x.relation.key).toArray(String[]::new);
		StringBuilder sizes = new StringBuilder();
		for (int i = 0; i < strSizes.length; i++)
			sizes.append(strSizes[i]).append(" ");
		StringBuilder allSizes = new StringBuilder();
		for (int i = 0; i < fldSizes.length; i++)
			allSizes.append(fldSizes[i]).append(" ");
		return String.join(" ", columns)  + "\n"
				+ String.join(" ", types_arr) + "\n"
				+ String.join(" ", specs_arr) + "\n"
				+ sizes.toString() + "\n"
				+ allSizes.toString() + "\n"
				+ columnar + "\n";
	}
}
