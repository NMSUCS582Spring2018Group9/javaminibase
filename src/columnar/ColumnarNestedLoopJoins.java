package columnar;

import java.io.IOException;
import java.util.Arrays;

import bufmgr.*;
import global.*;
import heap.*;
import index.*;
import iterator.*;

public class ColumnarNestedLoopJoins extends Iterator {
	private AttrType outerTypes[];
	private AttrType innerTypes[];
	private short innerStrSizes[];
	private CondExpr outputFilter[];
	private FldSpec innerMask[];
	private CondExpr rightFilter[];
	private FldSpec projection[];

	private Columnarfile columnarFile;

	private TupleScan inner;
	private Iterator outer;

	private boolean isComplete = false; // is the join complete
	private Tuple joinTuple = new Tuple(); // joined tuple
	
	private Tuple outerTuple = new Tuple();
	private Tuple innerTupleMasked;
	private boolean useOuter = true;

	/**
	 * constructor Initialize the two relations which are joined, including relation
	 * type,
	 * 
	 * @param innerTypes
	 *            Array containing field types of R.
	 * @param innerStrSizes
	 *            shows the length of the string fields.
	 * @param outerTypes
	 *            Array containing field types of S
	 * @param outerStrSizes
	 *            shows the length of the string fields.
	 * @param outerIterator
	 *            access method for left i/p to join
	 * @param relationName
	 *            access Columnar File for right i/p to join
	 * @param outputFilter
	 *            select expressions
	 * @param rightFilter
	 *            reference to filter applied on right i/p
	 * @param projection
	 *            shows what input fields go where in the output tuple
	 * @exception IOException
	 *                some I/O fault
	 * @exception NestedLoopException
	 *                exception from this class
	 */
	public ColumnarNestedLoopJoins(AttrType innerTypes[], short innerStrSizes[], AttrType outerTypes[],
			short outerStrSizes[], Iterator outerIterator, String relationName, CondExpr outputFilter[],
			CondExpr rightFilter[], FldSpec projection[]) throws IOException, NestedLoopException {

		this.outer = outerIterator;
		this.outerTypes = Arrays.copyOf(outerTypes, outerTypes.length);

		this.innerTypes = Arrays.copyOf(innerTypes, outerTypes.length);
		this.innerStrSizes = outerStrSizes;

		this.outputFilter = outputFilter;
		this.innerMask = Arrays.stream(outputFilter).filter(f -> f != null).map(f -> f.operand2.symbol)
				.toArray(FldSpec[]::new);
		this.rightFilter = rightFilter;

		this.projection = projection;

		try {
			TupleUtils.setup_op_tuple(joinTuple, new AttrType[projection.length], outerTypes, outerTypes.length,
					innerTypes, innerTypes.length, innerStrSizes, outerStrSizes, projection, projection.length);
		} catch (TupleUtilsException e) {
			throw new NestedLoopException(e, "TupleUtilsException is caught by ColumnarNestedLoopJoins.java");
		}

		try {
			columnarFile = new Columnarfile(relationName, innerTypes.length, innerTypes, outerStrSizes);
		} catch (Exception e) {
			throw new NestedLoopException(e, "Create new heapfile failed.");
		}
	}

	/**
	 * @return The joined tuple is returned
	 * @exception IOException
	 *                I/O errors
	 * @exception JoinsException
	 *                some join exception
	 * @exception IndexException
	 *                exception from super class
	 * @exception InvalidTupleSizeException
	 *                invalid tuple size
	 * @exception InvalidTypeException
	 *                tuple type not valid
	 * @exception PageNotReadException
	 *                exception from lower layer
	 * @exception TupleUtilsException
	 *                exception from using tuple utilities
	 * @exception PredEvalException
	 *                exception from PredEval class
	 * @exception SortException
	 *                sort exception
	 * @exception LowMemException
	 *                memory error
	 * @exception UnknowAttrType
	 *                attribute type unknown
	 * @exception UnknownKeyTypeException
	 *                key type unknown
	 * @exception Exception
	 *                other exceptions
	 * 
	 */
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		if (isComplete)
			return null;

		
		do {
			if (useOuter == true) {
				// Get a tuple from the outer, delete an existing scan on the file, and reopen a
				// new scan on the file.
				// If a get_next on the outer returns DONE, then the nested loops join is done
				// too.
				useOuter = false;
				if (inner != null) { // If this not the first time, then close
					inner.closetuplescan();
					inner = null;
				}

				try {
					inner = columnarFile.openTupleScan();
					
				} catch (Exception e) {
					throw new NestedLoopException(e, "openScan failed");
				}

				if ((outerTuple = outer.get_next()) == null) {
					if (inner != null) {
						inner.closetuplescan();
						inner = null;
					}
					isComplete = true;
					return null;
				}
			}

			// The next step is to get a tuple from the inner
			TID tid = new TID(innerTypes.length);
			while ((innerTupleMasked = inner.getNext(tid, innerMask)) != null) {
				innerTupleMasked.setHdr((short) innerTypes.length, innerTypes, innerStrSizes);

				if (PredEval.Eval(rightFilter, innerTupleMasked, null, innerTypes, null) == true) {
					if (PredEval.Eval(outputFilter, outerTuple, innerTupleMasked, outerTypes, innerTypes) == true) {
						// get the rest of the tuple
						Tuple innerTuple = columnarFile.getTuple(tid);
						
						// Apply a projection on the outer and inner tuples.
						Projection.Join(outerTuple, outerTypes, 
										innerTuple, innerTypes, 
										joinTuple, projection, projection.length);
						return joinTuple;
					}
				}
			}

			// There has been no match. Otherwise, we would have returned.
			// Hence, inner is exhausted, => set get_from_outer = TRUE, go to top of loop

			useOuter = true; // Loop back to top and get next outer tuple.
		} while (true);
	}

	/**
	 * implement the abstract method close() from super class Iterator to finish
	 * cleaning up
	 * 
	 * @exception IOException
	 *                I/O error from lower layers
	 * @exception JoinsException
	 *                join error from lower layers
	 * @exception IndexException
	 *                index access error
	 */
	public void close() throws JoinsException, IOException, IndexException {
		if (!closeFlag) {

			try {
				if (outer != null)
					outer.close();
				if (inner != null)
					inner.closetuplescan();
			} catch (Exception e) {
				throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
			}
			closeFlag = true;
		}
	}
}
