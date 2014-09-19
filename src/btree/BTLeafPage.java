package btree;

import java.io.IOException;

import global.*;
import heap.*;
import diskmgr.*;

public class BTLeafPage extends BTSortedPage {

	public BTLeafPage(int paramInt) throws ConstructPageException, IOException {
		super(paramInt);
		this.setType(NodeType.LEAF);
	}
	
	public BTLeafPage(Page paramPage, int paramInt) throws IOException{
		super(paramPage,paramInt);
		this.setType(NodeType.LEAF);
	}
	
	public BTLeafPage(PageId paramPage, int paramInt) throws ConstructPageException, IOException{
		super(paramPage,paramInt);
		this.setType(NodeType.LEAF);
	}
	
	public RID insertRecord( KeyClass key, RID dataRid ) throws InsertRecException{
		KeyDataEntry entry = new KeyDataEntry(key, dataRid);
		RID rid = insertRecord(entry);
		return rid;
	}//end method.
	
	public KeyDataEntry getFirst(RID rid) throws IOException, KeyNotMatchException, NodeNotMatchException, ConvertException, InvalidSlotNumberException{
		
		RID temp = firstRecord();
		if( temp == null ){
			return null;
		}
		rid.pageNo=temp.pageNo;
		rid.slotNo=temp.slotNo;
		Tuple tuple = getRecord( rid );
		KeyDataEntry toBeReturned = BT.getEntryFromBytes(tuple.getTupleByteArray(), tuple.getOffset(), tuple.getLength(), keyType, NodeType.LEAF);
		
		return toBeReturned;
	}//end method.
	
	public KeyDataEntry getNext(RID rid) throws IOException, InvalidSlotNumberException, KeyNotMatchException, NodeNotMatchException, ConvertException{
		
		RID next = nextRecord(rid);
		if( next == null ){
			return null;
		}
		rid.pageNo=next.pageNo;
		rid.slotNo=next.slotNo;
		Tuple tuple = getRecord( rid );
		KeyDataEntry toBeReturned = BT.getEntryFromBytes(tuple.getTupleByteArray(), tuple.getOffset(), tuple.getLength(), keyType, NodeType.LEAF);
		
		return toBeReturned;
	}//end method.
	
	public KeyDataEntry getCurrent(RID rid) throws InvalidSlotNumberException, IOException, KeyNotMatchException, NodeNotMatchException, ConvertException{
		Tuple tuple = getRecord( rid );
		KeyDataEntry toBeReturned = BT.getEntryFromBytes(tuple.getTupleByteArray(), tuple.getOffset(), tuple.getLength(), keyType, NodeType.LEAF);
		
		return toBeReturned;
	}//end method.
	
	public boolean delEntry( KeyDataEntry dEntry ) throws DeleteRecException{
		KeyDataEntry dataE;
		RID r = new RID();

		try {
			for (dataE = getFirst(r); dataE != null; dataE = getNext(r)) {
				if (dataE.equals(dEntry)) {
					if (deleteSortedRecord(r) == false)
						return false;
					return true;
				}
			}
		} catch (KeyNotMatchException | NodeNotMatchException
				| ConvertException | InvalidSlotNumberException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}//end method.
	
	
}//end class.
