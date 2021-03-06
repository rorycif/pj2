
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <cstring>
#include <vector>

#include "../rbf/rbfm.h"

using namespace std;

#define RM_EOF (-1)  // end of a scan operator

#define SUCCESS 0

//New
#define RM_FILE_DOES_NOT_EXIST 1
#define RM_FILE_ALREADY_EXIST 2
#define RM_DELETE_FAILED 3
#define RM_FILE_OPEN_FAILED 4
#define RM_INCORRECT_INPUT 5
#define RM_SEEK_FAILED 6
#define RM_WRITE_FAILED 7
#define RM_TABLE_DOES_NOT_EXIST 8
#define RM_TABLE_ALREADY_EXIST 9
#define RM_INSERT_FAILED 10
#define RM_READ_FAILED 11
#define RM_TABLE_IS_DELETED 12
#define RM_UPDATE_FAILED 13
#define RM_SCANFAILED 14

#define MAX_NAMESIZE 51
#define RM_RECORD_NOT_FOUND -1

// Catalog Tables
// Tables Catelog
typedef struct TablesCatalogHeader {
  uint32_t nextTableId; // id number for the next new record
  uint32_t freeSpaceOffset;
} TablesCatalogHeader;

typedef struct TablesCatalogEntry {
  Status flag;
  uint32_t tableId;
  char tableName[MAX_NAMESIZE];
  char fileName[MAX_NAMESIZE];
} TablesCatalogEntry;

// Columns Catelog
  typedef struct ColumnsCatalogHeader {
    uint32_t freeSpaceOffset;
  } ColumnsCatalogHeader;

typedef struct ColumnsCatalogEntry {
  Status flag;
  uint32_t tableId;
  char columnName[MAX_NAMESIZE];
  AttrType columnType;
  uint32_t columnLength;
  uint32_t columnPosition;
} ColumnsCatalogEntry;

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
 RM_ScanIterator() {};
 ~RM_ScanIterator() {};
 FileHandle * fhp;                     //pointer to a filehandle for method access
 string fileName;                      //the file where records lie
 vector <Attribute> SI_recordDescriptor;
 vector <string> targetAttributes;
 unsigned recordLength;
 unsigned nulls;
 vector<RID> records;
 // "data" follows the same format as RelationManager::insertTuple()
 RC getNextTuple(RID &rid, void *data);
 RC close();
};

// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);


protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;

  // catalog files name
  string tablesCatalogName = "tablesCatalog";
  string columnsCatalogName = "columnsCatalog";

  // ********************** Helper function **********************
  bool fileExists(const string &filename);
  RC getTableInfoByTableName(string tableName, TablesCatalogEntry * tablesCatalogEntry);
  RC isTableNameExistInCatalog(string tableName, FILE * tablesCatalogFile, uint32_t numOfRecords);

  // TablesCatalogEntry
  void updateTablesCatalogEntry(TablesCatalogEntry * tablesCatalogEntry, Status flag, uint32_t tableId, string tableName, string fileName);
  RC insertTablesCatalogEntries(FILE * pTablesFile, TablesCatalogEntry * tablesCatalogEntries, uint32_t numOfEntry);
  RC getTablesCatalogEntry(FILE * pTablesFile, uint32_t recordOffset, TablesCatalogEntry * tablesCatalogEntry);

  // TablesCatalogHeader
  void  initializeTablesCatalogHeader(TablesCatalogHeader * tablesCatalogHeader);
  void increaseTablesCatalogHeader(TablesCatalogHeader * tablescatalogHeader);
  RC insertTablesCatalogHeader(FILE * pTablesFile, TablesCatalogHeader * tablesCatalogHeader);
  RC getTablesCatalogHeader(TablesCatalogHeader * tablesCatalogHeader, FILE * pTablesFile);

  // ColumnsCatalogEntry
  void updateColumnsCatalogEntry(ColumnsCatalogEntry * columnsCatalogEntry, Status flag, uint32_t tableId, string columnName, AttrType columnType, uint32_t columnLength, uint32_t columnPosition);
  RC insertColumnsCatalogEntries(FILE * pColumnsFile, ColumnsCatalogEntry * columnsCatalogEntries, uint32_t numOfEntry, uint32_t targetOffset);
  RC getColumnsCatalogEntry(FILE * pColumnsFile, uint32_t recordOffset, ColumnsCatalogEntry * columnsCatalogEntry);
  RC findNextTargetRecord(FILE * pColumnsFile, uint32_t tableId, uint32_t head, uint32_t end, uint32_t * targetOffset);

  // ColumnsCatalogHeader
  void initializeColumnsCatalogHeader(ColumnsCatalogHeader * columnsCatalogHeader);
  void increaseColumnsCatalogHeader(ColumnsCatalogHeader * columnsCatalogHeader);
  RC insertColumnsCatalogHeader(FILE * pColumnsFile, ColumnsCatalogHeader * columnsCatalogHeader);
  RC getColumnsCatalogHeader(ColumnsCatalogHeader * columnCatalogHeader, FILE * pColumnsFile);
public:
  //  TBD -- Testing Function 
  void printTableCatalog();
  void printColumnsCatalog();
};

#endif
