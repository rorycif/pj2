#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <stdio.h>
#include <vector>
#include <climits>

#include "pfm.h"

#define INT_SIZE                4
#define REAL_SIZE               4
#define VARCHAR_LENGTH_SIZE     4

#define SUCCESS 0

#define RBFM_ATTRIBUTE_OFFSET_FAILURE 0
#define RBFM_CREATE_FAILED 1
#define RBFM_MALLOC_FAILED 2
#define RBFM_OPEN_FAILED   3
#define RBFM_APPEND_FAILED 4
#define RBFM_READ_FAILED   5
#define RBFM_WRITE_FAILED  6
#define RBFM_SLOT_DN_EXIST 7
#define RBFM_PAGE_DN_EXIST 8
#define RBFM_NOTHING_TO_DELETE 9
#define RBFM_CANT_UPDATE 10
#define RBFM_DELETE_ERROR 11
#define RBFM_UPDATE_FAIL 12
#define RBFM_ATTRIBUTE_DN_EXIST 13
#define RBFM_EMPTY_SCAN 14
#define RBFM_INVALID_RID 15
#define RBFM_EMPTY_PRINT 16
#define RBFM_INVALID_ATTRIBUTE 17

using namespace std;

// Record ID
typedef struct
{
    uint32_t pageNum; // page number
    uint32_t slotNum; // slot number in the page
} RID;


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum
{
    EQ_OP = 0,  // no condition// =
    LT_OP,      // <
    LE_OP,      // <=
    GT_OP,      // >
    GE_OP,      // >=
    NE_OP,      // !=
    NO_OP       // no condition
} CompOp;

typedef enum{     //used to check when deleting or updating
  dead =0,
  alive,
  moved,
} Status;

// Slot directory headers for page organization
// See chapter 9.6.2 of the cow book or lecture 3 slide 16 for more information
typedef struct SlotDirectoryHeader
{
    uint16_t freeSpaceOffset;
    uint16_t recordEntriesNumber;

} SlotDirectoryHeader;

// Assignment 2 tip: Make offset negative to represent a forwarding address
// Negative offset => length = page #, offset = -slot #
typedef struct SlotDirectoryRecordEntry
{
    uint32_t length;
    int32_t offset;
    Status statFlag;
    RID forwardAddress;                  //used if moved
} SlotDirectoryRecordEntry;

typedef SlotDirectoryRecordEntry* SlotDirectory;

typedef uint16_t ColumnOffset;

typedef uint16_t RecordLength;


/********************************************************************************
The scan iterator is NOT required to be implemented for the part 1 of the project
********************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {
public:
 FileHandle * fhp;                     //pointer to a filehandle for method access
 string fileName;                      //the file where records lie
 vector <Attribute> SI_recordDescriptor;
 vector<RID> records;
 unsigned recordLength;
 unsigned nulls;
 RBFM_ScanIterator() {};
 ~RBFM_ScanIterator() {};  string getFileName(const void * value, string conditionAttribute, CompOp compOp, vector<string> attributeNames, AttrType type);
 // Never keep the results in the memory. When getNextRecord() is called,
 // a satisfying record needs to be fetched from the file.
 // "data" follows the same format as RecordBasedFileManager::insertRecord().
 RC getNextRecord(RID &rid, void *data);
 RC close() { return -1; };
};


class RecordBasedFileManager
{
public:
  static RecordBasedFileManager* instance();

  RC createFile(const string &fileName);

  RC destroyFile(const string &fileName);

  RC openFile(const string &fileName, FileHandle &fileHandle);

  RC closeFile(FileHandle &fileHandle);

  //  Format of the data passed into the function is the following:
  //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
  //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
  //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
  //     Each bit represents whether each field value is null or not.
  //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
  //     If k-th bit from the left is set to 0, k-th field contains non-null values.
  //     If there are more than 8 fields, then you need to find the corresponding byte first,
  //     then find a corresponding bit inside that byte.
  //  2) Actual data is a concatenation of values of the attributes.
  //  3) For Int and Real: use 4 bytes to store the value;
  //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
  // For example, refer to the Q6 of Project 1 Environment document.
  RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

  RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);

  // This method will be mainly used for debugging/testing.
  // The format is as follows:
  // field1-name: field1-value  field2-name: field2-value ... \n
  // (e.g., age: 24  height: 6.1  salary: 9000
  //        age: NULL  height: 7.5  salary: 7500)
  RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

/******************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for the part 1 of the project
******************************************************************************************************************************************************************/
  RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

  // Assume the RID does not change after an update
  RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

  RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  RC scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator);
		
		
  	int getNullIndicatorSize(int fieldCount);
public:

protected:
  RecordBasedFileManager();
  ~RecordBasedFileManager();

private:
  static RecordBasedFileManager *_rbf_manager;
  static PagedFileManager *_pf_manager;

  // Private helper methods

  void newRecordBasedPage(void * page);

  SlotDirectoryHeader getSlotDirectoryHeader(void * page);
  void setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader);

  SlotDirectoryRecordEntry getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber);
  void setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry);

  unsigned getPageFreeSpaceSize(void * page);
  unsigned getRecordSize(const vector<Attribute> &recordDescriptor, const void *data);


  bool fieldIsNull(char *nullIndicator, int i);

  void setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data);
  void getRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, void *data);

  unsigned getAvailablePage(unsigned size, FileHandle &fileHandle);     //retuns the next availble page to take that size
  void compaction(void * pageData, SlotDirectoryHeader tempHeader, SlotDirectoryRecordEntry tempRecordEntry, unsigned shorten, unsigned slotNum);
  unsigned getAttributeOffset(const void * record, const vector<Attribute> &recordDescriptor, const string &attributeName);
  bool compareAttributes(const void * record, const void * value, AttrType type, string conditionAttribute, vector<Attribute> recordDescriptor, CompOp compOp);
  bool compareCheckInt(int val1, CompOp compOp, int val2);
  bool compareCheckFloat(float val1, CompOp compOp, float val2);
  bool compareCheckVarChar(string val1, CompOp compOp, string val2);

    //so that the iterater can use this class's private functions
    friend class RBFM_ScanIterator;
};

#endif
