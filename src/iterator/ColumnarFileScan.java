package iterator;

import java.io.IOException;

import bufmgr.*;
import global.*;
import heap.*;
import index.*;

public class ColumnarFileScan extends Iterator {

	public ColumnarFileScan(String file_name, AttrType[] attrs, short[] str_sizes, int num_out_fields, FldSpec[] fields,
			CondExpr[] filters) {
		// TODO
	}

	/**
	 * @return shows what input fields go where in the output tuple
	 */
	public FldSpec[] show() {
		return null;
	}

	/**
	 * @return the result tuple
	 */
	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * implement the abstract method close() from super class Iterator to finish
	 * cleaning up
	 */
	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		// TODO Auto-generated method stub

	}
}
