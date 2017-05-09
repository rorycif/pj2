
#include "rm.h"

#include <stdio.h>
#include <string>
#include <iostream>
#include <sys/stat.h>

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    /*
    // Check if the catalog files exist
    if (fileExists(tablesCatalogName)) {
        return TABLE_FILE_EXISTS;
    }

    if (fileExists(columnsCatalogName)) {
        return COLUMN_FILE_EXISTS;
    }
    */ 

    // Create the catalog files in disk
    FILE * pTablesFile = fopen(tablesCatalogName.c_str(), "wb");
    FILE * pColumnsFile = fopen(columnsCatalogName.c_str(), "wb");

    // Check if files open fail
    if (pTablesFile == NULL || pColumnsFile == NULL) {
        return FILE_OPEN_FAILED;
    }
    
	// Create the Headers and Entries
	TablesCatalogHeader * tablesCatalogHeader = new TablesCatalogHeader();
    ColumnsCatalogHeader * columnsCatalogHeader = new ColumnsCatalogHeader();
    
    // Add Table "Tables" to Tables Catalog
    TablesCatalogEntry * tablesCatalogEntry = new TablesCatalogEntry(0, "Tables", "Tables");
    insertTablesCatalogEntry(pTablesFile, tablesCatalogEntry, tablesCatalogEntry->tableId);
	updateTablesCatalogHeader(tablesCatalogHeader);
    
    // Add Table "Columns" to Table Catalog
	updateTablesCatalogEntry(tablesCatalogEntry, 1, "Columns", "Columns");
    insertTablesCatalogEntry(pTablesFile, tablesCatalogEntry, tablesCatalogEntry->tableId);
	updateTablesCatalogHeader(tablesCatalogHeader);

    // Add TablesCatalogHeader to the file
    insertTablesCatalogHeader(pTablesFile, tablesCatalogHeader);

    // Add conlumns info of each table to Columns Catalog
	ColumnsCatalogEntry * columnsCatalogEntry = new ColumnsCatalogEntry(1, "table-id", TypeInt, 4 , 1);
    insertColumnsCatalogEntry(pColumnsFile, columnsCatalogEntry, columnsCatalogHeader);
    updateColumnsCatalogHeader(columnsCatalogHeader);

    updateColumnsCatalogEntry(columnsCatalogEntry, 1, "table-name", TypeVarChar, 50, 2);
    insertColumnsCatalogEntry(pColumnsFile, columnsCatalogEntry, columnsCatalogHeader);
    updateColumnsCatalogHeader(columnsCatalogHeader);

    updateColumnsCatalogEntry(columnsCatalogEntry, 1, "file-name", TypeVarChar, 50, 3);
    insertColumnsCatalogEntry(pColumnsFile, columnsCatalogEntry, columnsCatalogHeader);
    updateColumnsCatalogHeader(columnsCatalogHeader);
    
    updateColumnsCatalogEntry(columnsCatalogEntry, 2, "table-id", TypeInt, 4, 1);
    insertColumnsCatalogEntry(pColumnsFile, columnsCatalogEntry, columnsCatalogHeader);
    updateColumnsCatalogHeader(columnsCatalogHeader);
    
    updateColumnsCatalogEntry(columnsCatalogEntry, 2, "column-name", TypeVarChar, 50, 2);
    insertColumnsCatalogEntry(pColumnsFile, columnsCatalogEntry, columnsCatalogHeader);
    updateColumnsCatalogHeader(columnsCatalogHeader);

    updateColumnsCatalogEntry(columnsCatalogEntry, 2, "column-type", TypeInt, 4, 3);
    insertColumnsCatalogEntry(pColumnsFile, columnsCatalogEntry, columnsCatalogHeader);
    updateColumnsCatalogHeader(columnsCatalogHeader);

    updateColumnsCatalogEntry(columnsCatalogEntry, 2, "column-length", TypeInt, 4, 4);
    insertColumnsCatalogEntry(pColumnsFile, columnsCatalogEntry, columnsCatalogHeader);
    updateColumnsCatalogHeader(columnsCatalogHeader);

    updateColumnsCatalogEntry(columnsCatalogEntry, 2, "column-position", TypeInt, 4, 5);
    insertColumnsCatalogEntry(pColumnsFile, columnsCatalogEntry, columnsCatalogHeader);
    updateColumnsCatalogHeader(columnsCatalogHeader);

	// write columnsCatalogHeader to disk
	insertColumnsCatalogHeader(pColumnsFile, columnsCatalogHeader);

	// free the pointers
    fclose(pTablesFile);
    fclose(pColumnsFile);

    // TBD
    FILE * pFile = fopen(tablesCatalogName.c_str(), "wb");
    fseek(pFile, sizeof(TablesCatalogHeader), SEEK_SET);
    void * raw = malloc(sizeof(tablesCatalogEntry));
    fread(raw, sizeof(tablesCatalogEntry), 1, pFile);
    TablesCatalogEntry * data = (TablesCatalogEntry *) raw;
    cout << data->fileName << endl;
	fclose(pFile);

	delete(tablesCatalogHeader);
	delete(tablesCatalogEntry);
	delete(columnsCatalogHeader);
	delete(columnsCatalogEntry);

    return SUCCESS;
}

RC RelationManager::deleteCatalog()
{
    return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
}

// ********************** Helper function **********************
bool RelationManager::fileExists(const string &filename)
{
    // If stat fails, we can safely assume the file doesn't exist
    struct stat sb;
    return stat(filename.c_str(), &sb) == 0;
}

void RelationManager::updateTablesCatalogEntry(TablesCatalogEntry * tablesCatalogEntry, uint32_t tableId, string tableName, string fileName)
{
	tablesCatalogEntry->tableId = tableId;
	tablesCatalogEntry->tableName = tableName;
	tablesCatalogEntry->fileName = fileName;
}

void RelationManager::insertTablesCatalogEntry(FILE * pTablesFile, TablesCatalogEntry * tablesCatalogEntry, uint32_t entryId) 
{
    fseek(pTablesFile, (sizeof(TablesCatalogHeader) + entryId * sizeof(TablesCatalogEntry)), SEEK_SET);
    fwrite(tablesCatalogEntry, 1, sizeof(TablesCatalogEntry), pTablesFile);
    fflush(pTablesFile);
}
/*
TablesCatalogEntry RelationManager::getTablesCatalogEntry(void * pTablesFile, uint32_t entryId) 
{
    TablesCatalogEntry tablesCatalogEntry;
    memcpy(
        &tablesCatalogEntry,
        ((char *)pTablesFile + sizeof(TablesCatalogHeader) + entryId * sizeof(tablesCatalogEntry)),
        sizeof(TablesCatalogEntry)
    );
    return tablesCatalogEntry;
}
*/

void RelationManager::updateTablesCatalogHeader(TablesCatalogHeader * tablescatalogHeader)
{
	tablescatalogHeader->nextTableId += 1;
	tablescatalogHeader->numOfTables += 1;
	tablescatalogHeader->freeSpaceOffset += sizeof(TablesCatalogEntry);
}


void RelationManager::insertTablesCatalogHeader(FILE * pTablesFile, TablesCatalogHeader * tablesCatalogHeader) 
{
    fseek(pTablesFile, 0, SEEK_SET);
    fwrite(tablesCatalogHeader, 1, sizeof(TablesCatalogHeader), pTablesFile);
    fflush(pTablesFile);
}

void RelationManager::getTablesCatalogHeader(TablesCatalogHeader * tablesCatalogHeader, void * pTablesFile) 
{
    memcpy(
        tablesCatalogHeader,
        pTablesFile,
        sizeof(TablesCatalogHeader)
    );
}

void RelationManager::updateColumnsCatalogEntry(ColumnsCatalogEntry * columnsCatalogEntry, uint32_t tableId, string columnName,
											AttrType columnType, uint32_t columnLength, uint32_t columnPosition)
 {
    columnsCatalogEntry->tableId = tableId;
    columnsCatalogEntry->columnName = columnName;
    columnsCatalogEntry->columnType = columnType;
    columnsCatalogEntry->columnLength = columnType;
    columnsCatalogEntry->columnPosition = columnPosition;
 }

void RelationManager::insertColumnsCatalogEntry(FILE * pColumnsFile, ColumnsCatalogEntry * columnsCatalogEntry,
												ColumnsCatalogHeader * columnsCatalogHeader)
{
	uint32_t freeSpaceOffset = columnsCatalogHeader->freeSpaceOffset;

    fseek(pColumnsFile, freeSpaceOffset, SEEK_SET);
    fwrite(columnsCatalogEntry, 1, sizeof(columnsCatalogEntry), pColumnsFile);
    fflush(pColumnsFile);
}
/*
ColumnsCatalogHeader RelationManager::getColumnsCatalogEntry(void * pColumnsFile)
{
	// TBC
	return -1;
}
*/

void RelationManager::updateColumnsCatalogHeader(ColumnsCatalogHeader * columnsCatalogHeader)
{
	columnsCatalogHeader->freeSpaceOffset += sizeof(ColumnsCatalogHeader);
}

void RelationManager::insertColumnsCatalogHeader(FILE * pColumnsFile, ColumnsCatalogHeader * columnsCatalogHeader)
{
    fseek(pColumnsFile, 0, SEEK_SET);
    fwrite(columnsCatalogHeader, 1, sizeof(ColumnsCatalogHeader), pColumnsFile);
    fflush(pColumnsFile);
}

void RelationManager::getColumnsCatalogHeader(ColumnsCatalogHeader * columnCatalogHeader, void * pColumnsFile)
{
	memcpy(
		columnCatalogHeader,
		pColumnsFile,
		sizeof(ColumnsCatalogHeader)
	);
}
