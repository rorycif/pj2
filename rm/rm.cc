
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
    
    // Check if the catalog files exist
    if (fileExists(tablesCatalogName)) {
        return FILE_EXISTS;
    }

    if (fileExists(columnsCatalogName)) {
        return FILE_EXISTS;
    }

    // Create the catalog files in disk
    FILE * pTablesFile = fopen(tablesCatalogName.c_str(), "wb");
    FILE * pColumnsFile = fopen(columnsCatalogName.c_str(), "wb");

    // Check if files open fail
    if (pTablesFile == NULL || pColumnsFile == NULL) {
        return FILE_OPEN_FAILED;
    }

	// Create the Headers and Entries
	TablesCatalogHeader tablesCatalogHeader;
    ColumnsCatalogHeader columnsCatalogHeader;
    TablesCatalogEntry tablesCatalogEntry;
    ColumnsCatalogEntry columnsCatalogEntry;

    // initizlize the Headers
    initializeTablesCatalogHeader(&tablesCatalogHeader);
    initializeColumnsCatalogHeader(&columnsCatalogHeader);
    
    // Add Table "Tables" to Tables Catalog
    updateTablesCatalogEntry(&tablesCatalogEntry, 0, "Tables", "Tables");
    if (insertTablesCatalogEntry(pTablesFile, &tablesCatalogEntry))
        return INSERT_FAILED;
    updateTablesCatalogHeader(&tablesCatalogHeader);

    // Add Table "Columns" to Table Catalog
	updateTablesCatalogEntry(&tablesCatalogEntry, 1, "Columns", "Columns");
    if (insertTablesCatalogEntry(pTablesFile, &tablesCatalogEntry))
        return INSERT_FAILED;
	updateTablesCatalogHeader(&tablesCatalogHeader);

    // Add TablesCatalogHeader to the file
    if (insertTablesCatalogHeader(pTablesFile, &tablesCatalogHeader))
        return INSERT_FAILED;

    // Add conlumns info of each table to Columns Catalog
    updateColumnsCatalogEntry(&columnsCatalogEntry, 1, "table-id", TypeInt, 4 , 1);
    if (insertColumnsCatalogEntry(pColumnsFile, &columnsCatalogEntry, &columnsCatalogHeader))
        return INSERT_FAILED;
    updateColumnsCatalogHeader(&columnsCatalogHeader);

    updateColumnsCatalogEntry(&columnsCatalogEntry, 1, "table-name", TypeVarChar, 50, 2);
    if (insertColumnsCatalogEntry(pColumnsFile, &columnsCatalogEntry, &columnsCatalogHeader))
        return INSERT_FAILED;
    updateColumnsCatalogHeader(&columnsCatalogHeader);

    updateColumnsCatalogEntry(&columnsCatalogEntry, 1, "file-name", TypeVarChar, 50, 3);
    if (insertColumnsCatalogEntry(pColumnsFile, &columnsCatalogEntry, &columnsCatalogHeader))
        return INSERT_FAILED;
    updateColumnsCatalogHeader(&columnsCatalogHeader);
    
    updateColumnsCatalogEntry(&columnsCatalogEntry, 2, "table-id", TypeInt, 4, 1);
    if (insertColumnsCatalogEntry(pColumnsFile, &columnsCatalogEntry, &columnsCatalogHeader))
        return INSERT_FAILED;
    updateColumnsCatalogHeader(&columnsCatalogHeader);
    
    updateColumnsCatalogEntry(&columnsCatalogEntry, 2, "column-name", TypeVarChar, 50, 2);
    if (insertColumnsCatalogEntry(pColumnsFile, &columnsCatalogEntry, &columnsCatalogHeader))
        return INSERT_FAILED;
    updateColumnsCatalogHeader(&columnsCatalogHeader);

    updateColumnsCatalogEntry(&columnsCatalogEntry, 2, "column-type", TypeInt, 4, 3);
    if (insertColumnsCatalogEntry(pColumnsFile, &columnsCatalogEntry, &columnsCatalogHeader))
        return INSERT_FAILED;
    updateColumnsCatalogHeader(&columnsCatalogHeader);

    updateColumnsCatalogEntry(&columnsCatalogEntry, 2, "column-length", TypeInt, 4, 4);
    if (insertColumnsCatalogEntry(pColumnsFile, &columnsCatalogEntry, &columnsCatalogHeader))
        return INSERT_FAILED;
    updateColumnsCatalogHeader(&columnsCatalogHeader);

    updateColumnsCatalogEntry(&columnsCatalogEntry, 2, "column-position", TypeInt, 4, 5);
    if (insertColumnsCatalogEntry(pColumnsFile, &columnsCatalogEntry, &columnsCatalogHeader))
        return INSERT_FAILED;
    updateColumnsCatalogHeader(&columnsCatalogHeader);

	// write columnsCatalogHeader to disk
    if (insertColumnsCatalogHeader(pColumnsFile, &columnsCatalogHeader))
        return INSERT_FAILED;

	// free the pointers
    fclose(pTablesFile);
    fclose(pColumnsFile);

    return SUCCESS;
}

RC RelationManager::deleteCatalog()
{   
    if (fileExists(tablesCatalogName.c_str()) == 0)
        return FILE_DOES_NOT_EXIST;

    if (fileExists(columnsCatalogName.c_str()) == 0)
        return FILE_DOES_NOT_EXIST;

    if (remove(tablesCatalogName.c_str()) != 0)
        return DELETE_FAILED;

    if (remove(columnsCatalogName.c_str()) != 0)
        return DELETE_FAILED;

    return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    if (fileExists(tableName.c_str()))
        return FILE_EXISTS;

    // create the RBF file with tablename
    RecordBasedFileManager * _rbfm = RecordBasedFileManager::instance();
    if (_rbfm->createFile(tableName))
        return RBFM_CREATE_FAILED;

    // check if the catalog files exist
    if (fileExists(tablesCatalogName) == 0)
        return FILE_DOES_NOT_EXIST;

    if (fileExists(columnsCatalogName) == 0)
        return FILE_DOES_NOT_EXIST;
    
    // open and read the headers from disk
    FILE * pTablesFile = fopen(tablesCatalogName.c_str(), "rb+");
    FILE * pColumnsFile = fopen(columnsCatalogName.c_str(), "rb+");

    TablesCatalogHeader tempTablesCatalogHeader;
    ColumnsCatalogHeader tempColumnsCatalogHeader;
    uint32_t tableId;

    if (getTablesCatalogHeader(&tempTablesCatalogHeader, pTablesFile))
        return READ_FAILED;

    if (getColumnsCatalogHeader(&tempColumnsCatalogHeader, pColumnsFile))
        return READ_FAILED;

    // insert the table into tables catalog on disk
    TablesCatalogEntry tempTablesCatalogEntry;
    tableId = tempTablesCatalogHeader.nextTableId;
    updateTablesCatalogEntry(&tempTablesCatalogEntry, tableId, tableName, tableName);
    if (insertTablesCatalogEntry(pTablesFile, &tempTablesCatalogEntry))
        return INSERT_FAILED;

    // update the header of tables catalog on disk
    updateTablesCatalogHeader(&tempTablesCatalogHeader);
    if (insertTablesCatalogHeader(pTablesFile, &tempTablesCatalogHeader))
        return INSERT_FAILED;

    // insert the column attributes into tables catalog on dsik
    ColumnsCatalogEntry tempColumnsCatalogEntry;
    for(uint16_t i = 0; i < attrs.size(); i++) 
    {
        // TBC
        // create the entry
        // insert the entry
        // update the header in memory 
    }

    // TBC
    // update the header on disk

    // close the file
    fclose(pTablesFile);
    fclose(pColumnsFile);

    return SUCCESS;
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

RC RelationManager::insertTablesCatalogEntry(FILE * pTablesFile, TablesCatalogEntry * tablesCatalogEntry) 
{
    uint32_t tableId = tablesCatalogEntry->tableId;

    if (fseek(pTablesFile, (sizeof(TablesCatalogHeader) + tableId * sizeof(TablesCatalogEntry)), SEEK_SET))
        return SEEK_FAILED;
    
    if (fwrite(tablesCatalogEntry, sizeof(TablesCatalogEntry), 1, pTablesFile))
    {
        fflush(pTablesFile);
        return SUCCESS;
    }

    return WRITE_FAILED;
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

void  RelationManager::initializeTablesCatalogHeader(TablesCatalogHeader * tablesCatalogHeader)
{
    tablesCatalogHeader->nextTableId = 0;
    tablesCatalogHeader->numOfTables = 0;
    tablesCatalogHeader->freeSpaceOffset = sizeof(TablesCatalogHeader);
}

void RelationManager::updateTablesCatalogHeader(TablesCatalogHeader * tablescatalogHeader)
{
	tablescatalogHeader->nextTableId += 1;
	tablescatalogHeader->numOfTables += 1;
	tablescatalogHeader->freeSpaceOffset += sizeof(TablesCatalogEntry);
}


RC RelationManager::insertTablesCatalogHeader(FILE * pTablesFile, TablesCatalogHeader * tablesCatalogHeader) 
{
    rewind(pTablesFile);

    if (fwrite(tablesCatalogHeader, sizeof(TablesCatalogHeader), 1, pTablesFile))
    {
        fflush(pTablesFile);
        return SUCCESS;
    }

    return WRITE_FAILED;
}

RC RelationManager::getTablesCatalogHeader(TablesCatalogHeader * tablesCatalogHeader, FILE * pTablesFile) 
{
    // read header from the disk
    void * raw = malloc(sizeof(TablesCatalogHeader));
    if (fread(raw, sizeof(TablesCatalogHeader), 1, pTablesFile) != 1)
        return READ_FAILED;

    // copy the header to memory 
    memcpy(
        tablesCatalogHeader,
        raw,
        sizeof(TablesCatalogHeader)
    );

    free(raw);
    return SUCCESS;
}

void RelationManager::updateColumnsCatalogEntry(ColumnsCatalogEntry * columnsCatalogEntry, uint32_t tableId, string columnName,
											AttrType columnType, uint32_t columnLength, uint32_t columnPosition)
 {
    columnsCatalogEntry->tableId = tableId;
    columnsCatalogEntry->columnName = columnName;
    columnsCatalogEntry->columnType = columnType;
    columnsCatalogEntry->columnLength = columnLength;
    columnsCatalogEntry->columnPosition = columnPosition;
 }

RC RelationManager::insertColumnsCatalogEntry(FILE * pColumnsFile, ColumnsCatalogEntry * columnsCatalogEntry,
												ColumnsCatalogHeader * columnsCatalogHeader)
{
	uint32_t freeSpaceOffset = columnsCatalogHeader->freeSpaceOffset;

    if (fseek(pColumnsFile, freeSpaceOffset, SEEK_SET))
        return SEEK_FAILED;

    if (fwrite(columnsCatalogEntry, sizeof(ColumnsCatalogEntry), 1, pColumnsFile))
    {
        fflush(pColumnsFile);
        return SUCCESS;
    }

    return WRITE_FAILED;
}
/*
ColumnsCatalogHeader RelationManager::getColumnsCatalogEntry(void * pColumnsFile)
{
	// TBC
	return -1;
}
*/

void RelationManager::initializeColumnsCatalogHeader(ColumnsCatalogHeader * columnsCatalogHeader)
{
    columnsCatalogHeader->freeSpaceOffset = sizeof(ColumnsCatalogHeader);
}


void RelationManager::updateColumnsCatalogHeader(ColumnsCatalogHeader * columnsCatalogHeader)
{
	columnsCatalogHeader->freeSpaceOffset += sizeof(ColumnsCatalogEntry);
}

RC RelationManager::insertColumnsCatalogHeader(FILE * pColumnsFile, ColumnsCatalogHeader * columnsCatalogHeader)
{
    rewind(pColumnsFile);

    if (fwrite(columnsCatalogHeader, sizeof(ColumnsCatalogHeader), 1, pColumnsFile))
    {
        fflush(pColumnsFile);
        return SUCCESS;
    }

    return WRITE_FAILED;
}

RC RelationManager::getColumnsCatalogHeader(ColumnsCatalogHeader * columnsCatalogHeader, FILE * pColumnsFile)
{
    // read header from the disk
    void * raw = malloc(sizeof(ColumnsCatalogHeader));
    if (fread(raw, sizeof(ColumnsCatalogHeader), 1, pColumnsFile) != 1)
        return READ_FAILED;

    // copy the header to memory
    memcpy(
        columnsCatalogHeader,
        raw,
        sizeof(ColumnsCatalogHeader)
    );

    free(raw);
    return SUCCESS;
}
