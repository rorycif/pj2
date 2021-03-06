#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>

#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    // Initialize the internal PagedFileManager instance
    _pf_manager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName)
{
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;
    newRecordBasedPage(firstPageData);

    // Adds the first record based page.
    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName)
{
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle)
{
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid)
{
    // Gets the size of the record.
    unsigned recordSize = getRecordSize(recordDescriptor, data);
    // Cycles through pages looking for enough free space for the new entry.
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    bool pageFound = false;
    unsigned i;
    unsigned numPages = fileHandle.getNumberOfPages();
    for (i = 0; i < numPages; i++)
    {
        if (fileHandle.readPage(i, pageData))
            return RBFM_READ_FAILED;

        // When we find a page with enough space (accounting also for the size that will be added to the slot directory), we stop the loop.
        if (getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + recordSize)
        {
            pageFound = true;
            break;
        }
    }

    // If we can't find a page with enough space, we create a new one
    if(!pageFound)
    {
        newRecordBasedPage(pageData);
    }

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    // Setting the return RID.
    rid.pageNum = i;
    rid.slotNum = slotHeader.recordEntriesNumber;

    // Adding the new record reference in the slot directory.
    SlotDirectoryRecordEntry newRecordEntry;
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
    newRecordEntry.statFlag = alive;                                   //freshly inserted so alive
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    slotHeader.recordEntriesNumber += 1;
    setSlotDirectoryHeader(pageData, slotHeader);

    // Adding the record data.
    setRecordAtOffset (pageData, newRecordEntry.offset, recordDescriptor, data);

    // Writing the page to disk.
    if (pageFound)
    {
        if (fileHandle.writePage(i, pageData))
            return RBFM_WRITE_FAILED;
    }
    else
    {
        if (fileHandle.appendPage(pageData))
            return RBFM_APPEND_FAILED;
    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{
    // Retrieve the specific page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    switch (recordEntry.statFlag) {
        case dead:          //reading a dead record
        {
            free(pageData);
            return RBFM_INVALID_RID;
            break;
        }
        case alive:
        {
            getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);
            free(pageData);
            return SUCCESS;
            break;
        }
        case moved:     //find it in foward address
        {
            free(pageData);
            return readRecord(fileHandle,recordDescriptor,recordEntry.forwardAddress,data);
            break;
        }

    }
    return RBFM_READ_FAILED;        //should not get here
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data)
{
    if(!data)
      return RBFM_EMPTY_PRINT;
    // Parse the null indicator into an array
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);

    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    cout << "----" << endl;
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        cout << setw(10) << left << recordDescriptor[i].name << ": ";
        // If the field is null, don't print it
        bool isNull = fieldIsNull(nullIndicator, i);
        if (isNull)
        {
            cout << "NULL" << endl;
            continue;
        }
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                uint32_t data_integer;
                memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
                offset += INT_SIZE;

                cout << "" << data_integer << endl;
            break;
            case TypeReal:
                float data_real;
                memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
                offset += REAL_SIZE;

                cout << "" << data_real << endl;
            break;
            case TypeVarChar:
                // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
                uint32_t varcharSize;
                memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;

                // Gets the actual string.
                char *data_string = (char*) malloc(varcharSize + 1);
                if (data_string == NULL)
                    return RBFM_MALLOC_FAILED;
                memcpy(data_string, ((char*) data + offset), varcharSize);

                // Adds the string terminator.
                data_string[varcharSize] = '\0';
                offset += varcharSize;

                cout << data_string << endl;
                free(data_string);
            break;
        }
    }
    cout << "----" << endl;

    return SUCCESS;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
//    cout<< "deleting record\n";
//    cout<< "delete: rid "<<rid.pageNum<< " "<< rid.slotNum<<endl;
    if (fileHandle.getNumberOfPages() < rid.pageNum){       //correct page error check
        return RBFM_PAGE_DN_EXIST;
    }
    void * pageData = malloc(PAGE_SIZE);                //where we will extract page
    fileHandle.readPage(rid.pageNum, pageData);        //extracting page
    SlotDirectoryHeader  tempHeader = getSlotDirectoryHeader(pageData);     //get page slot directory
    if (rid.slotNum > tempHeader.recordEntriesNumber)       //slot error check
        return RBFM_SLOT_DN_EXIST;
//    cout<< "header says there are this many records: "<< tempHeader.recordEntriesNumber<<endl;
    SlotDirectoryRecordEntry tempRecordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);    //get record entry
//    cout<< "record lies at this offset: "<< tempRecordEntry.offset<< " with this length: "<< tempRecordEntry.length<<endl;
    switch(tempRecordEntry.statFlag){
        case dead:
//            cout<< "dead delete\n";
            free(pageData);
            return RBFM_NOTHING_TO_DELETE;               //nothing to delete
            break;
        case alive:
//            cout<< "alive delete\n";
            compaction(pageData, tempHeader, tempRecordEntry, tempRecordEntry.length, rid.slotNum);     //remove from page
            fileHandle.writePage(rid.pageNum,pageData);                                                 //write page
//            cout<< "post write page\n";
            free(pageData);
//            cout<< "post free page\n";
            return SUCCESS;
            break;
        case moved:
            free(pageData);
            return deleteRecord(fileHandle,recordDescriptor,tempRecordEntry.forwardAddress);  //recursive call
            break;
    }
    return RBFM_DELETE_ERROR;    //should not get here but gets rid of compile warning
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){
    //cout<< "in update\n";
    if (fileHandle.getNumberOfPages() < rid.pageNum){       //correct page error check
        return RBFM_PAGE_DN_EXIST;
    }
    void * pageData = malloc(PAGE_SIZE);                //where we will extract page
    fileHandle.readPage(rid.pageNum, pageData);        //extracting page
    SlotDirectoryHeader  tempHeader = getSlotDirectoryHeader(pageData);     //get page slot directory
    if (rid.slotNum > tempHeader.recordEntriesNumber)       //slot error check
        return RBFM_SLOT_DN_EXIST;
    SlotDirectoryRecordEntry tempRecordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);    //get record entry
    switch (tempRecordEntry.statFlag) {
        case dead:
//            cout<< "dead\n";
            free(pageData);
            return RBFM_CANT_UPDATE;
            break;

        case moved:
//            cout<< "moved\n";
            free(pageData);
            return updateRecord(fileHandle,recordDescriptor,data,tempRecordEntry.forwardAddress);
            break;
        case alive:
//            cout<< "alive\n";
            unsigned size = getRecordSize(recordDescriptor, data);
            if (size == tempRecordEntry.length){
                //cout<< "same length just update\n";
                setRecordAtOffset(pageData, tempRecordEntry.offset, recordDescriptor, data);
				fileHandle.writePage(rid.pageNum,pageData);
				free(pageData);
				return SUCCESS;
            }
            else if(size < tempRecordEntry.length){     //smaller size so compact
                setRecordAtOffset(pageData, tempRecordEntry.offset, recordDescriptor, data);
                compaction(pageData, tempHeader, tempRecordEntry, tempRecordEntry.length - size, rid.slotNum);
				        tempRecordEntry = getSlotDirectoryRecordEntry(pageData,rid.slotNum);
                fileHandle.writePage(rid.pageNum, pageData);
                free(pageData);
                return SUCCESS;
            }
            else{       //record is now bigger
                //cout<< "record is bigger so expand "<< size <<" > "<< tempRecordEntry.length<<endl;
                deleteRecord(fileHandle,recordDescriptor, rid);     //clear space
                tempRecordEntry.statFlag = moved;
                if (insertRecord(fileHandle, recordDescriptor, data, tempRecordEntry.forwardAddress)){
                  free(pageData);
                  return RBFM_UPDATE_FAIL;
                }
                fileHandle.readPage(tempRecordEntry.forwardAddress.pageNum,pageData);
                setSlotDirectoryRecordEntry(pageData, rid.slotNum, tempRecordEntry);    //update forwardAddress
                fileHandle.writePage(rid.pageNum, pageData);
                free(pageData);

                return SUCCESS;
            }
            break;
    }
    return RBFM_CANT_UPDATE;    //should not make it here but to remove compiler warning
}


RC RecordBasedFileManager::scan(FileHandle &fileHandle,
   const vector<Attribute> &recordDescriptor,
   const string &conditionAttribute,
   const CompOp compOp,                  // comparision type such as "<" and "="
   const void *value,                    // used in the comparison
   const vector<string> &attributeNames, // a list of projected attributes
   RBFM_ScanIterator &rbfm_ScanIterator){
     //validations
      cout<< "in rbfm scan\n";
     unsigned pageCount = fileHandle.getNumberOfPages();
     cout<< "page count: "<< pageCount<<endl;
     cout<< "size: "<<recordDescriptor.size()<<endl;
     if (!pageCount || !recordDescriptor.size() || attributeNames.size() > recordDescriptor.size())
       return RBFM_EMPTY_SCAN;
      cout<< "passed first verification\n";
     cout<< conditionAttribute<<endl;
     AttrType targetType;
     bool noOperation = false;
     if (compOp == NO_OP && conditionAttribute == "")    //valid call
       noOperation = true;
     bool isInVector = false;                      //used to find if the condition name exist in record descriptor vector
     for (unsigned i =0; i < recordDescriptor.size(); i++){
       if (recordDescriptor[i].name == conditionAttribute){
//          cout<< "type determind\n";
         targetType = recordDescriptor[i].type;
         isInVector = true;
       }
     }
     if (!isInVector && !noOperation)
       return RBFM_ATTRIBUTE_DN_EXIST;
//      cout<< "passed second verification\n";      //iterate though pages
     void * currentPage;
     void * currentRecord;
     RID tempRid;
     SlotDirectoryHeader tempHeader;
     SlotDirectoryRecordEntry tempRecordEntry;
     for (unsigned i =0; i < pageCount; i++){
//        cout<<"scaning page "<<i<<endl;
       currentPage = malloc(PAGE_SIZE);
       fileHandle.readPage(i,currentPage);
       tempHeader = getSlotDirectoryHeader(currentPage);
       //iterate though each record on page
       for (unsigned j =0; j < tempHeader.recordEntriesNumber; j ++){
//          cout<< "iterating through record "<< j<<endl;
         tempRecordEntry = getSlotDirectoryRecordEntry(currentPage,j);
         currentRecord = malloc(tempRecordEntry.length);
         rbfm_ScanIterator.recordLength = tempRecordEntry.length;
         rbfm_ScanIterator.nulls = getNullIndicatorSize(recordDescriptor.size());
         getRecordAtOffset(currentPage, tempRecordEntry.offset, recordDescriptor, currentRecord);
         //compare the record for condition
         if (compareAttributes(currentRecord, value, targetType, conditionAttribute, recordDescriptor, compOp)){
           tempRid.pageNum =i;
           tempRid.slotNum =j;
           rbfm_ScanIterator.records.push_back(tempRid);     //the record's location is saved to iterator
         }
         free (currentRecord);
       }
       free(currentPage);
//        cout<< "moving on to next page\n";
     }
   rbfm_ScanIterator.fhp = &fileHandle;          //so scan iterater knows which file to read from
   rbfm_ScanIterator.SI_recordDescriptor = recordDescriptor;
     return SUCCESS;
   }

//helper function when a new page with enough space is needed
unsigned RecordBasedFileManager::getAvailablePage(unsigned size, FileHandle &fileHandle){
  void* tempPage = malloc(PAGE_SIZE);           //temppage that we will fetch
  SlotDirectoryHeader tempHeader;
  for (unsigned i =0; i < fileHandle.getNumberOfPages(); i ++){
    fileHandle.readPage(i, tempPage);
    tempHeader = getSlotDirectoryHeader(tempPage);
    if (PAGE_SIZE -tempHeader.freeSpaceOffset - sizeof(SlotDirectoryRecordEntry)){
      //there is enough space to suport the insert and a new SlotDirectoryRecordEntry
      free (tempPage);
      return i;
    }
  }
  free (tempPage);
  return fileHandle.getNumberOfPages();    //a new page must be made to support
}

void RecordBasedFileManager::compaction(void * pageData, SlotDirectoryHeader tempHeader, SlotDirectoryRecordEntry tempRecordEntry, unsigned shorten, unsigned slotNum){
//    cout<< "compaction called\n";
    unsigned begin = tempRecordEntry.offset + tempRecordEntry.length - shorten;
//    cout<< "begining "<<begin <<endl;
    unsigned end = tempRecordEntry.offset + tempRecordEntry.length;
//    cout<< "end "<< end<< endl;
    memmove((char *)pageData + begin, (char *)pageData + end, PAGE_SIZE - end);
	if (tempRecordEntry.length == shorten)
		tempRecordEntry.statFlag = dead;    //marked dead
    tempRecordEntry.length -= shorten;
//    cout<< "new size "<<tempRecordEntry.length<<endl;
    unsigned oldOffset = tempHeader.freeSpaceOffset;
    tempHeader.freeSpaceOffset += shorten;         //update page free space
//    cout<< "new free space: "<< tempHeader.freeSpaceOffset<<endl;
    setSlotDirectoryHeader(pageData,tempHeader);                                  //commit header
    setSlotDirectoryRecordEntry(pageData, slotNum, tempRecordEntry);      //commit dead slot
    //shifting + updating records offset
//    cout<< "shifting the records from: " <<oldOffset<< " to "<<tempHeader.freeSpaceOffset<<endl;
    memmove((char *)pageData  + tempHeader.freeSpaceOffset, (char *)pageData + oldOffset, PAGE_SIZE - oldOffset- shorten);
//    cout<< "after move\n";
    for (unsigned i = slotNum; i < tempHeader.recordEntriesNumber; i ++){
//      cout<< "fixing this record's offset "<< i<<endl;
      tempRecordEntry = getSlotDirectoryRecordEntry(pageData, i);
//	  cout<< "from this offset "<< tempRecordEntry.offset<<endl;
      tempRecordEntry.offset += shorten;
      setSlotDirectoryRecordEntry(pageData,i,tempRecordEntry);
//	  cout<< "to this "<< tempRecordEntry.offset<<endl;
    }
//  cout<< "done with compating\n";
}


RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){
  //page validation
  if (rid.pageNum >= fileHandle.getNumberOfPages())
    return RBFM_PAGE_DN_EXIST;
  void * pageData = malloc(PAGE_SIZE);
  fileHandle.readPage(rid.pageNum, pageData);
  SlotDirectoryHeader tempHeader = getSlotDirectoryHeader(pageData);
  //attribute type validation
  bool found = false;
  unsigned AttPos =0;
  for (unsigned i =0; i < recordDescriptor.size(); i++){
    if (recordDescriptor[i].name == attributeName){
      found = true;
      AttPos = i;
    }
  }
  if(!found)
    return RBFM_INVALID_ATTRIBUTE;
  //slot validation
  if (rid.slotNum > tempHeader.recordEntriesNumber)
    return RBFM_SLOT_DN_EXIST;
  //stat flag validation
  SlotDirectoryRecordEntry tempRecordEntry = getSlotDirectoryRecordEntry(pageData,rid.slotNum);
  switch (tempRecordEntry.statFlag) {
    case alive:     //return the attribute
    {
      void * fullRecord = malloc(tempRecordEntry.length);
      unsigned attributeOffset =0;
      if (readRecord(fileHandle,recordDescriptor,rid,fullRecord))     //reading record shoudl work
        return RBFM_READ_FAILED;
      unsigned nullIdicatorBytes =getNullIndicatorSize(recordDescriptor.size());
      void * nulls = malloc(nullIdicatorBytes);
      memcpy(nulls,fullRecord, nullIdicatorBytes);
      if (fieldIsNull((char*)fullRecord, AttPos)){   //return null string
        string out = "NULL";
        char * ptr = &out[0u];
        memcpy(data, nulls, nullIdicatorBytes);
        memcpy(data + nullIdicatorBytes, &out, 4);
        free(fullRecord);
        free(pageData);
        return SUCCESS;
      }
//      printRecord(recordDescriptor, fullRecord);
      char * record = (char *) fullRecord;
      attributeOffset = getAttributeOffset(fullRecord, recordDescriptor, attributeName);
      record += attributeOffset;          //move to position of record;
      switch (recordDescriptor[AttPos].type) {
        case TypeInt:
        {
          int * tempInt = (int *)record;      //cast
          int value = tempInt[0];
//          cout<< "int value: "<<value<<endl;
          memcpy(data,nulls, nullIdicatorBytes);
          memcpy(data + nullIdicatorBytes, &value, INT_SIZE);
          break;
        }
        case TypeReal:
        {
          float * tempFloat = (float *)record;
          memcpy(data,nulls,nullIdicatorBytes);
          memcpy(data + nullIdicatorBytes, &tempFloat[0], REAL_SIZE);
          break;
        }
        case TypeVarChar:
        {
          int * cast = (int *)record;     //casting for size
          int size = cast[0];
          record += INT_SIZE;
          string out = "";
          for (int i =0; i < size; i++){      //concating string
            out += record[0];
            record += 1;
          }
          char* ptr = &out[0u];
          memcpy(data, nulls, nullIdicatorBytes);
          memcpy(data + nullIdicatorBytes, &size, INT_SIZE);        //add the size of return value
          memcpy(data + INT_SIZE + nullIdicatorBytes, ptr, size);   //add the actual data
          break;
        }
      }
      free(nulls);
      free(pageData);
      free(fullRecord);
      return SUCCESS;
      break;
    }
    case dead:      //error
    {
      cout<< "dead slot\n";
      free(pageData);
      return RBFM_INVALID_RID;
      break;
    }
    case moved:     //recurssive call to fowarding address
    {
      cout<< "moved slot\n";
      free(pageData);
      return readAttribute(fileHandle, recordDescriptor, tempRecordEntry.forwardAddress, attributeName, data);
      break;
    }
  }
  free(pageData);       //should not reach here idealy
  return SUCCESS;
}

// Private helper methods

//returns where the wanted attribute starts
unsigned RecordBasedFileManager::getAttributeOffset(const void * record, const vector<Attribute> &recordDescriptor, const string &attributeName){
//  cout<< "attibute helper function called\n";
  unsigned count =0;
  char * temp = (char *)record;                       //to keep data pointer unchanged
  char * temp2 = (char*)record;
  int nullBytes = getNullIndicatorSize(recordDescriptor.size());
//  cout<< "null bytes = "<<nullBytes<<endl;
  count += nullBytes;             //add bytes to running sum
  temp += nullBytes;
  int varSize = 0;                //for varchar sizes
  int * intTemp;                  //used to cast data stream to int
  for (unsigned i =0; i < recordDescriptor.size(); i ++){
      if(!fieldIsNull(temp2, i)){
        if (recordDescriptor[i].name == attributeName){      //return where attribute starts
//          cout<< "returning count\n";
          return count;
        }
        switch (recordDescriptor[i].type){
          case TypeInt:     //move over int attribute
          {
//            cout<< "going over int\n";
            count += INT_SIZE;
            temp += INT_SIZE;
            break;
          }
          case TypeReal:    //move over real attribute
          {
//            cout<< "going over real\n";
            count += REAL_SIZE;
            temp += REAL_SIZE;
            break;
          }
          case TypeVarChar:   //move over varchar attribute
          {
//            cout<< "going over var char\n";
            intTemp = (int *)temp;
            varSize = intTemp[0];
            count += INT_SIZE;
            count += varSize;
            temp += INT_SIZE;
            temp += varSize;
            break;
          }
        }
      }
  }


  return -1;
}

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void * page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    SlotDirectoryHeader slotHeader;
    slotHeader.freeSpaceOffset = PAGE_SIZE;
    slotHeader.recordEntriesNumber = 0;
    setSlotDirectoryHeader(page, slotHeader);
}

SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void * page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader)
{
    // Setting the slot directory header.
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy  (
            &recordEntry,
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            sizeof(SlotDirectoryRecordEntry)
            );

    return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
    // Setting the slot directory entry data.
    memcpy  (
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            &recordEntry,
            sizeof(SlotDirectoryRecordEntry)
            );
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page)
{
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Offset into *data. Start just after null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize to size of header
    unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                size += INT_SIZE;
                offset += INT_SIZE;
            break;
            case TypeReal:
                size += REAL_SIZE;
                offset += REAL_SIZE;
            break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                size += varcharSize;
                offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    return size;
}

// Calculate actual bytes for nulls-indicator for the given field counts
int RecordBasedFileManager::getNullIndicatorSize(int fieldCount)
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset (nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Points to start of record
    char *start = (char*) page + offset;

    // Offset into *data
    unsigned data_offset = nullIndicatorSize;
    // Offset into page header
    unsigned header_offset = 0;

    RecordLength len = recordDescriptor.size();
    memcpy(start + header_offset, &len, sizeof(len));
    header_offset += sizeof(len);

    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;

    // Keeps track of the offset of each record
    // Offset is relative to the start of the record and points to the END of a field
    ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!fieldIsNull(nullIndicator, i))
        {
            // Points to current position in *data
            char *data_start = (char*) data + data_offset;

            // Read in the data for the next column, point rec_offset to end of newly inserted data
            switch (recordDescriptor[i].type)
            {
                case TypeInt:
                    memcpy (start + rec_offset, data_start, INT_SIZE);
                    rec_offset += INT_SIZE;
                    data_offset += INT_SIZE;
                break;
                case TypeReal:
                    memcpy (start + rec_offset, data_start, REAL_SIZE);
                    rec_offset += REAL_SIZE;
                    data_offset += REAL_SIZE;
                break;
                case TypeVarChar:
                    unsigned varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                    memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                    // We also have to account for the overhead given by that integer.
                    rec_offset += varcharSize;
                    data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
                break;
            }
        }
        // Copy offset into record header
        // Offset is relative to the start of the record and points to END of field
        memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
        header_offset += sizeof(ColumnOffset);
    }
}

// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
// Memset null indicator as 1?
void RecordBasedFileManager::getRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, void *data)
{
    // Pointer to start of record
    char *start = (char*) page + offset;

    // Allocate space for null indicator. The returned null indicator may be larger than
    // the null indicator in the table has had fields added to it
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
    memcpy(data, nullIndicator, nullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write data to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;

    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;

        // Grab pointer to end of this column
        ColumnOffset endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

        // rec_offset keeps track of start of column, so end-start = total size
        uint32_t fieldSize = endPointer - rec_offset;

        // Special case for varchar, we must give data the size of varchar first
        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
            data_offset += VARCHAR_LENGTH_SIZE;
        }
        // Next we copy bytes equal to the size of the field and increase our offsets
        memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
        rec_offset += fieldSize;
        data_offset += fieldSize;
    }
}

//gives each scan file a unique name
string RBFM_ScanIterator::getFileName(const void * value, string conditionAttribute, CompOp compOp, vector<string> attributeNames, AttrType type){
  cout<< "making output name \n";
  string name = "";
  name += conditionAttribute;
  switch (compOp) {
    case EQ_OP:
      name += " = ";
      break;
    case LT_OP:
      name += " < ";
      break;
    case LE_OP:
      name += " <= ";
      break;
    case GT_OP:
      name += " > ";
      break;
    case GE_OP:
      name += " >= ";
      break;
    case NE_OP:
      name += " != ";
      break;
    case NO_OP:
      name += " noOp ";
      break;
  }
    switch (type) {
      case TypeInt:
      {
        int * tempint = (int *)value;
        name += to_string(tempint[0]);
        break;
      }
      case TypeReal:
      {
        float * tempfloat = (float *)value;
        name += to_string(tempfloat[0]);
        break;
      }
      case TypeVarChar:
      {
        char * tempChar = (char *)value;
        name += tempChar;
        break;
      }
    }
    for (unsigned i = 0; i < attributeNames.size(); i ++){  //output names
      name += " ";
      name += attributeNames[i];
    }
  return name;
}

//if a record meets the critera this will return true
bool RecordBasedFileManager::compareAttributes(const void * record, const void * value, AttrType type, string conditionAttribute, vector<Attribute> recordDescriptor, CompOp compOp){
  //get attribute location
//  cout<< "comparing attributes\n";
  unsigned start = getAttributeOffset(record, recordDescriptor, conditionAttribute);
  char * recordCast = (char*)record;              //avoid compiler errors
  if (conditionAttribute == ""){
//    cout<< "no attribute comparison\n";
    return true;
  }
  //cast to valid type
  switch (type) {
    case TypeInt:       //comparing intergers
    {
      recordCast += start;
      int * tempInt = (int*)recordCast;       //first operand
      int * tempInt2 = (int*)value;           //second opperand
      return compareCheckInt(tempInt[0], compOp, tempInt2[0]);
      break;
    }
    case TypeReal:      //comparing real numbers
    {
      recordCast += start;
      float * tempFloat = (float*)recordCast;       //first operand
      float * tempFloat2 = (float*)value;           //second opperand
      return compareCheckFloat(tempFloat[0], compOp, tempFloat2[0]);
      break;
    }
    case TypeVarChar:   //comparing strings
    {
      recordCast += start;
      //int * size = (int *)recordCast;
      recordCast += INT_SIZE;
      string tempString = "";
      tempString += recordCast;
      string tempString2 = (char*)value;
      return compareCheckVarChar(tempString, compOp, tempString2);
      break;
    }
  }
  //return comparison truth value
  return false;       //should not get here ideally
}
//helper function for comaring intergers
bool RecordBasedFileManager::compareCheckInt(int val1, CompOp compOp, int val2){
  switch (compOp) {
    case EQ_OP:   //equals
    {
      return val1 == val2;
      break;
    }
    case LT_OP:   //less than
    {
      return val1 < val2;
      break;
    }
    case LE_OP:   //less than or equal to
    {
      return val1 <= val2;
      break;
    }
    case GT_OP:   //greater than
    {
      return val1 > val2;
      break;
    }
    case GE_OP:   //greater than or equal to
    {
      return val1 >= val2;
      break;
    }
    case NE_OP:   //not equal to
    {
      return val1 != val2;
      break;
    }
    case NO_OP:   //no operation
    {
      return true;
      break;
    }
  }
  return false; //should not reach here
}
//helperfunction for comparing floats
bool RecordBasedFileManager::compareCheckFloat(float val1, CompOp compOp, float val2){
  switch (compOp) {
    case EQ_OP:   //equals
    {
      return val1 == val2;
      break;
    }
    case LT_OP:   //less than
    {
      return val1 < val2;
      break;
    }
    case LE_OP:   //less than or equal to
    {
      return val1 <= val2;
      break;
    }
    case GT_OP:   //greater than
    {
      return val1 > val2;
      break;
    }
    case GE_OP:   //greater than or equal to
    {
      return val1 >= val2;
      break;
    }
    case NE_OP:   //not equal to
    {
      return val1 != val2;
      break;
    }
    case NO_OP:   //no operation
    {
      return true;
      break;
    }
  }
  return false; //should not reach here
}
//helper function for comparing strings
bool RecordBasedFileManager::compareCheckVarChar(string val1, CompOp compOp, string val2){
  switch (compOp) {
    case EQ_OP:   //equals
    {
      return val1 == val2;
      break;
    }
    case LT_OP:   //less than
    {
      return val1 < val2;
      break;
    }
    case LE_OP:   //less than or equal to
    {
      return val1 <= val2;
      break;
    }
    case GT_OP:   //greater than
    {
      return val1 > val2;
      break;
    }
    case GE_OP:   //greater than or equal to
    {
      return val1 >= val2;
      break;
    }
    case NE_OP:   //not equal to
    {
      return val1 != val2;
      break;
    }
    case NO_OP:   //no operation
    {
      return true;
      break;
    }
  }
  return false; //should not reach here
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data){
    cout<< "scan iterator reading from file\n";
    //no instances in iterator
    if (!records.size()){
      return RBFM_EOF;
    }
    //read page from rid and its slot descriptor
    RecordBasedFileManager * _rbfm = RecordBasedFileManager::instance();
    void * tempPage = malloc(PAGE_SIZE);
    fhp->readPage(rid.pageNum,tempPage);
    SlotDirectoryRecordEntry tempRecordEntry = _rbfm->getSlotDirectoryRecordEntry(tempPage,rid.slotNum);
    //extract record to data pointer
    _rbfm->getRecordAtOffset(tempPage, tempRecordEntry.offset, SI_recordDescriptor, data);
    free(tempPage);
    if (rid.pageNum == records[records.size() -1].pageNum && rid.slotNum == records[records.size()-1].slotNum){
        return RBFM_EOF;                //last element was called
    }
    return SUCCESS;
}
