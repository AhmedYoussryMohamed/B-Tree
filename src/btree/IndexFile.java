package btree;

import java.io.*;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.DiskMgrException;
import global.*;
import heap.InvalidSlotNumberException;

/**
 * Base class for a index file
 */
public abstract class IndexFile 
{
  /**
   * Insert entry into the index file.
   * @param data the key for the entry
   * @param rid the rid of the tuple with the key
 * @throws DiskMgrException 
 * @throws BufMgrException 
 * @throws PageNotReadException 
 * @throws PageUnpinnedException 
 * @throws PagePinnedException 
 * @throws InvalidFrameNumberException 
 * @throws HashEntryNotFoundException 
 * @throws ReplacerException 
 * @throws HashOperationException 
 * @throws BufferPoolExceededException 
 * @throws DeleteRecException 
 * @throws InsertRecException 
 * @throws InvalidSlotNumberException 
 * @throws ConvertException 
 * @throws NodeNotMatchException 
 * @throws KeyNotMatchException 
 * @throws ConstructPageException 
 * @throws IOException 
   */
  abstract public void insert(final KeyClass data, final RID rid) throws IOException, ConstructPageException, KeyNotMatchException, NodeNotMatchException, ConvertException, InvalidSlotNumberException, InsertRecException, DeleteRecException, BufferPoolExceededException, HashOperationException, ReplacerException, HashEntryNotFoundException, InvalidFrameNumberException, PagePinnedException, PageUnpinnedException, PageNotReadException, BufMgrException, DiskMgrException;
  
  /**
   * Delete entry from the index file.
   * @param data the key for the entry
   * @param rid the rid of the tuple with the key
 * @throws DeleteRecException 
 * @throws InvalidSlotNumberException 
 * @throws ConvertException 
 * @throws NodeNotMatchException 
 * @throws KeyNotMatchException 
 * @throws ConstructPageException 
 * @throws IOException 
 * @throws InvalidFrameNumberException 
 * @throws HashEntryNotFoundException 
 * @throws PageUnpinnedException 
 * @throws ReplacerException 
   */
  abstract public boolean Delete(final KeyClass data, final RID rid) throws IOException, ConstructPageException, KeyNotMatchException, NodeNotMatchException, ConvertException, InvalidSlotNumberException, DeleteRecException, ReplacerException, PageUnpinnedException, HashEntryNotFoundException, InvalidFrameNumberException;
}
