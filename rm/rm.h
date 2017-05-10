
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"

using namespace std;

#define RM_EOF (-1)  // end of a scan operator

#define SUCCESS 0

#define TABLE_FILE_EXISTS 1
#define COLUMN_FILE_EXISTS 2
#define FILE_OPEN_FAILED 3
#define DELETE_FAILED 4

#define SEEK_FAILED 1
#define WRITE_FAILED 2
#define INSERT_FAILED 3

// Catalog Tables
// Tables Catelog
typedef struct TablesCatalogHeader {
  uint32_t nextTableId; // id number for the next new record
  uint32_t numOfTables;
  uint32_t freeSpaceOffset;
} TablesCatalogHeader;

typedef struct TablesCatalogEntry {
  uint32_t tableId;
  string tableName;
  string fileName;
} TablesCatalogEntry;

// Columns Catelog
typedef struct ColumnsCatalogHeader {
  uint32_t freeSpaceOffset;
} ColumnsCatalogHeader;

typedef struct ColumnsCatalogEntry {
  uint32_t tableId;
  string columnName;
  AttrType columnType;
  uint32_t columnLength;
  uint32_t columnPosition;
} ColumnsCatalogEntry;

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data) { return RM_EOF; };
  RC close() { return -1; };
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
  string columnsCatalogName = "columnCatalog";

  // ********************** Helper function **********************
  bool fileExists(const string &filename);
  
  // TablesCatalogEntry
  void updateTablesCatalogEntry(TablesCatalogEntry * tablesCatalogEntry, uint32_t tableId, string tableName, string fileName);
  RC insertTablesCatalogEntry(FILE * pTablesFile, TablesCatalogEntry * tablesCatalogEntry);
  //TablesCatalogEntry getTablesCatalogEntry(void * pTablesFile, uint32_t entryId);

  // TablesCatalogHeader
  void  initializeTablesCatalogHeader(TablesCatalogHeader * tablesCatalogHeader);
  void updateTablesCatalogHeader(TablesCatalogHeader * tablescatalogHeader);
  RC insertTablesCatalogHeader(FILE * pTablesFile, TablesCatalogHeader * tablesCatalogHeader);
  void getTablesCatalogHeader(TablesCatalogHeader * tablesCatalogHeader, void * pTablesFile);

  // ColumnsCatalogEntry
  void updateColumnsCatalogEntry(ColumnsCatalogEntry * columnsCatalogEntry, uint32_t tableId, string columnName, AttrType columnType, uint32_t columnLength, uint32_t columnPosition);
  RC insertColumnsCatalogEntry(FILE * pColumnsFile, ColumnsCatalogEntry * columnsCatalogEntry, ColumnsCatalogHeader * columnsCatalogHeader);
  //TablesCatalogHeader getColumnsCatalogEntry(void * pTablesFile);

  // ColumnsCatalogHeader
  void initializeColumnsCatalogHeader(ColumnsCatalogHeader * columnsCatalogHeader);
  void updateColumnsCatalogHeader(ColumnsCatalogHeader * columnsCatalogHeader);
  RC insertColumnsCatalogHeader(FILE * pColumnsFile, ColumnsCatalogHeader * columnsCatalogHeader);
  void getColumnsCatalogHeader(ColumnsCatalogHeader * columnCatalogHeader, void * pColumnsFile);
};

#endif
