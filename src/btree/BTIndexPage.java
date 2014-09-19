package btree;

import global.*;
import heap.InvalidSlotNumberException;
import heap.Tuple;

import java.io.IOException;

import diskmgr.Page;

public class BTIndexPage extends BTSortedPage {
	
	public BTIndexPage(int paramInt) throws ConstructPageException, IOException {
		super(paramInt);
		this.setType(NodeType.INDEX);
	}
	
	public BTIndexPage(Page paramPage, int paramInt) throws IOException{
		super(paramPage,paramInt);
		this.setType(NodeType.INDEX);
	}
	
	public BTIndexPage(PageId paramPage, int paramInt) throws ConstructPageException, IOException{
		super(paramPage,paramInt);
		this.setType(NodeType.INDEX);
	}
	
	public RID insertKey( KeyClass key , PageId pageNo) throws InsertRecException{
		KeyDataEntry entry = new KeyDataEntry(key, pageNo);
		return insertRecord( entry );
	}//end method.
	
	public PageId getPageNoByKey( KeyClass key ) throws KeyNotMatchException, NodeNotMatchException, ConvertException, InvalidSlotNumberException, IOException{
		RID rid = new RID();
		KeyDataEntry entry = getFirst(rid);
		PageId pgid = new PageId();
		PageId prevPgid = getLeftLink();
//		System.out.println("!!!!!!!!!!!!!!!               " + entry.key +"                           !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		while( entry != null ){
			IndexData indData = (IndexData) entry.data;
			pgid = indData.getData();
			
//			int compare = BT.keyCompare( key, entry.key );
			int compare = BT.keyCompare( entry.key, key );
			if( compare < 0 ){
				prevPgid.pid = pgid.pid;
				entry = getNext(rid);
			}else if( compare == 0 ){
				return pgid;
			}else{
				//positive.
				return prevPgid;
			}//end else.
			
		}//end while.
		
		return pgid;
	}//end method.
	
	public KeyDataEntry getFirst(RID rid) throws IOException, KeyNotMatchException, NodeNotMatchException, ConvertException, InvalidSlotNumberException{
		RID temp = firstRecord();
		if( temp == null ){
			return null;
		}
		rid.pageNo=temp.pageNo;
		rid.slotNo=temp.slotNo;
		Tuple tuple = getRecord( rid );
		KeyDataEntry toBeReturned = BT.getEntryFromBytes(tuple.getTupleByteArray(), tuple.getOffset(), tuple.getLength(), keyType, NodeType.INDEX);
		
		return toBeReturned;
	}//end method.
	
	public KeyDataEntry getNext( RID rid ) throws InvalidSlotNumberException, IOException, KeyNotMatchException, NodeNotMatchException, ConvertException{
		RID next = nextRecord(rid);
		if( next == null ){
			return null;
		}
		rid.pageNo=next.pageNo;
		rid.slotNo=next.slotNo;
		Tuple tuple = getRecord( rid );
		KeyDataEntry toBeReturned = BT.getEntryFromBytes(tuple.getTupleByteArray(), tuple.getOffset(), tuple.getLength(), keyType, NodeType.INDEX);
		
		return toBeReturned;
	}//end method.
	
	public PageId getLeftLink() throws IOException{
		
		return this.getPrevPage();
	}//end method.
	
	public void setLeftLink( PageId left ) throws IOException{
		
		this.setPrevPage( left );
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
