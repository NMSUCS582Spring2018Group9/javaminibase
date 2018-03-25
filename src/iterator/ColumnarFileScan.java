package iterator;

import java.io.IOException;

import bufmgr.*;
import columnar.*;
import global.*;
import heap.*;
import index.*;

public class ColumnarFileScan extends Iterator {

	private short[] string_sizes;
	private AttrType[] attributes;
	private FldSpec[] fields;
	private Columnarfile file;
	private TupleScan scanner;
	private CondExpr[] filters;

	private Tuple in = new Tuple();
	private Tuple out = new Tuple();

	private boolean closed = false;

	/**
	 * 
	 * @param file_name
	 *            columnarfile to open
	 * @param attrs
	 *            attributes for the input fields
	 * @param str_sizes
	 *            the length of string attribute fields
	 * @param fields
	 *            a mapping of the fields to the output tuples
	 * @param filters
	 *            select expressions
	 * @exception IOException
	 *                some I/O fault
	 * @exception FileScanException
	 *                exception from this class
	 * @exception TupleUtilsException
	 *                exception from this class
	 * @exception InvalidRelation
	 *                invalid relation
	 */
	public ColumnarFileScan(String file_name, AttrType[] attrs, short[] str_sizes, FldSpec[] fields, CondExpr[] filters)
			throws TupleUtilsException, InvalidRelation, IOException, FileScanException {
		this.attributes = attrs;
		this.string_sizes = str_sizes;
		this.filters = filters;
		this.fields = fields;

		AttrType[] res_attrs = new AttrType[fields.length];
		TupleUtils.setup_op_tuple(out, res_attrs, attrs, attrs.length, str_sizes, fields, fields.length);

		try {
			in.setHdr((short) attrs.length, attrs, str_sizes);
		} catch (Exception e) {
			throw new FileScanException(e, "setHdr() failed");
		}

		try {
			file = new Columnarfile(file_name, attrs.length, attrs, str_sizes);
		} catch (Exception e) {
			throw new FileScanException(e, "Create new Columnarfile failed");
		}

		try {
			scanner = file.openTupleScan();
		} catch (Exception e) {
			throw new FileScanException(e, "openTupleScan() failed");
		}

	}

	/**
	 * @return shows what input fields go where in the output tuple
	 */
	public FldSpec[] show() {
		return fields;
	}

	/**
	 * @return the result tuple
	 * @exception JoinsException
	 *                some join exception
	 * @exception IOException
	 *                I/O errors
	 * @exception InvalidTupleSizeException
	 *                invalid tuple size
	 * @exception InvalidTypeException
	 *                tuple type not valid
	 * @exception PageNotReadException
	 *                exception from lower layer
	 * @exception PredEvalException
	 *                exception from PredEval class
	 * @exception UnknowAttrType
	 *                attribute type unknown
	 * @exception FieldNumberOutOfBoundException
	 *                array out of bounds
	 * @exception WrongPermat
	 *                exception for wrong FldSpec argument
	 */
	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {

		TID id = new TID(this.attributes.length);
		while (true) {
			in = scanner.getNext(id);
			if (in == null)
				return null;

			in.setHdr((short) attributes.length, attributes, string_sizes);
			if (!PredEval.Eval(filters, in, null, attributes, null))
				continue;

			Projection.Project(in, attributes, out, fields, fields.length);
			return out;

		}
	}

	/**
	 * implement the abstract method close() from super class Iterator to finish
	 * cleaning up
	 */
	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		if (closed)
			return; // if already closed, do nothing

		scanner.closetuplescan();
		closed = true;
	}
}
