
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
        return RM_FILE_ALREADY_EXIST;
    }

    if (fileExists(columnsCatalogName)) {
        return RM_FILE_ALREADY_EXIST;
    }

    // Create the catalog files in disk
    FILE * pTablesCatalogFile = fopen(tablesCatalogName.c_str(), "wb");
    FILE * pColumnsCatalogFile = fopen(columnsCatalogName.c_str(), "wb");

    // Check if files open fail
    if (pTablesCatalogFile == NULL || pColumnsCatalogFile == NULL) {
        return RM_FILE_OPEN_FAILED;
    }

	// Create and initialize the Headers
	TablesCatalogHeader tablesCatalogHeader;
    ColumnsCatalogHeader columnsCatalogHeader;
    initializeTablesCatalogHeader(&tablesCatalogHeader);
    initializeColumnsCatalogHeader(&columnsCatalogHeader);

    // Create an array to temporarily store the records catalogs
    const uint32_t numOfTablesCatalogEntry = 2;
    const uint32_t numOfColumnsCatalogEntry = 8;
    TablesCatalogEntry tablesCatalogEntries[numOfTablesCatalogEntry];
    ColumnsCatalogEntry columnsCatalogEntries[numOfColumnsCatalogEntry];

    // **************** Update Tables Catalog ****************
    // Store the records of table catalog into array
    updateTablesCatalogEntry(&tablesCatalogEntries[0], 1, "Tables", "Tables");
    increaseTablesCatalogHeader(&tablesCatalogHeader);

	updateTablesCatalogEntry(&tablesCatalogEntries[1], 2, "Columns", "Columns");
    increaseTablesCatalogHeader(&tablesCatalogHeader);

    // Write the header and the records in array to tables catalog on disk
    if (insertTablesCatalogHeader(pTablesCatalogFile, &tablesCatalogHeader))
        return RM_WRITE_FAILED;
    if (insertTablesCatalogEntries(pTablesCatalogFile, tablesCatalogEntries, numOfTablesCatalogEntry))
        return RM_WRITE_FAILED;

    // **************** Update Columns Catalog ****************
    // Store the records of columns catalog into array
    // records of Tables Calog
    uint32_t initialFreeSpaceOffset = columnsCatalogHeader.freeSpaceOffset;
    updateColumnsCatalogEntry(&columnsCatalogEntries[0], 1, "table-id", TypeInt, 4 , 1);
    increaseColumnsCatalogHeader(&columnsCatalogHeader);

    updateColumnsCatalogEntry(&columnsCatalogEntries[1], 1, "table-name", TypeVarChar, 50, 2);
    increaseColumnsCatalogHeader(&columnsCatalogHeader);

    updateColumnsCatalogEntry(&columnsCatalogEntries[2], 1, "file-name", TypeVarChar, 50, 3);
    increaseColumnsCatalogHeader(&columnsCatalogHeader);

    // records of Columns Calog
    updateColumnsCatalogEntry(&columnsCatalogEntries[3], 2, "table-id", TypeInt, 4, 1);
    increaseColumnsCatalogHeader(&columnsCatalogHeader);

    updateColumnsCatalogEntry(&columnsCatalogEntries[4], 2, "column-name",  TypeVarChar, 50, 2);
    increaseColumnsCatalogHeader(&columnsCatalogHeader);

    updateColumnsCatalogEntry(&columnsCatalogEntries[5], 2, "column-type", TypeInt, 4, 3);
    increaseColumnsCatalogHeader(&columnsCatalogHeader);

    updateColumnsCatalogEntry(&columnsCatalogEntries[6], 2, "column-length", TypeInt, 4, 4);
    increaseColumnsCatalogHeader(&columnsCatalogHeader);
    
    updateColumnsCatalogEntry(&columnsCatalogEntries[7], 2, "column-position", TypeInt, 4, 5);
    increaseColumnsCatalogHeader(&columnsCatalogHeader);

    // Write the header and the records in array to columns catalog on disk
    if (insertColumnsCatalogHeader(pColumnsCatalogFile, &columnsCatalogHeader))
        return RM_WRITE_FAILED;
    if (insertColumnsCatalogEntries(pColumnsCatalogFile, columnsCatalogEntries, numOfColumnsCatalogEntry, initialFreeSpaceOffset))
        return RM_WRITE_FAILED;

	// free the pointers
    fclose(pTablesCatalogFile);
    fclose(pColumnsCatalogFile);

    return SUCCESS;
}

RC RelationManager::deleteCatalog()
{
    if (!fileExists(tablesCatalogName))
        return RM_FILE_DOES_NOT_EXIST;

    if (!fileExists(columnsCatalogName))
        return FILE_DOES_NOT_EXIST;

    if (remove(tablesCatalogName.c_str()))
        return RM_DELETE_FAILED;

    if (remove(columnsCatalogName.c_str()))
        return RM_DELETE_FAILED;

    return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    // check if any file is already named tableName on disk
    if (fileExists(tableName))
        return RM_FILE_ALREADY_EXIST;

    // check if the catalog files exist
    if (!fileExists(tablesCatalogName))
        return RM_FILE_DOES_NOT_EXIST;

    if (!fileExists(columnsCatalogName))
        return RM_FILE_DOES_NOT_EXIST;

    // Read tables catalog to find if the table exist
    FILE * pTablesCatalogFile = fopen(tablesCatalogName.c_str(), "rb+");
    TablesCatalogHeader tempTablesCatalogHeader;
    TablesCatalogEntry tempTablesCatalogEntry;
    uint32_t numOfRecords;

    if (getTablesCatalogHeader(&tempTablesCatalogHeader, pTablesCatalogFile))
        return READ_FAILED;

    // check if table is already created
    numOfRecords = tempTablesCatalogHeader.numOfRecords;
    if (isTableNameExistInCatalog(tableName, pTablesCatalogFile, numOfRecords))
    {
        fclose(pTablesCatalogFile);
        return RM_TABLE_ALREADY_EXIST;
    }

    // *************** Insert to Tables Catalog ***************
    // insert the table into tables catalog on disk
    uint32_t tableId = tempTablesCatalogHeader.nextTableId;
    updateTablesCatalogEntry(&tempTablesCatalogEntry, tableId, tableName, tableName);
    if (insertTablesCatalogEntries(pTablesCatalogFile, &tempTablesCatalogEntry, 1))
        return RM_INSERT_FAILED;

    increaseTablesCatalogHeader(&tempTablesCatalogHeader);
    if (insertTablesCatalogHeader(pTablesCatalogFile, &tempTablesCatalogHeader))
        return RM_INSERT_FAILED;

    // *************** Insert to Columns Catalog ***************
    // insert the column attributes into columns catalog on dsik
    uint32_t numOfAttrs = attrs.size();
    ColumnsCatalogEntry tempColumnsCatalogEntries[numOfAttrs];
    string columnName;
    AttrType columnType;
    uint32_t columnLength;

    FILE * pColumnsCatalogFile = fopen(columnsCatalogName.c_str(), "rb+");
    ColumnsCatalogHeader tempColumnsCatalogHeader;

    if (getColumnsCatalogHeader(&tempColumnsCatalogHeader, pColumnsCatalogFile))
        return RM_READ_FAILED;

    // insert each columns attribute to disk
    uint32_t initialFreeSpaceOffset = tempColumnsCatalogHeader.freeSpaceOffset;
    for(uint16_t i = 0; i < numOfAttrs; i++)
    {
        columnName = attrs[i].name;
        columnType = attrs[i].type;
        columnLength = attrs[i].length;
        updateColumnsCatalogEntry(&tempColumnsCatalogEntries[i], tableId, columnName, columnType, columnLength, i);
        increaseColumnsCatalogHeader(&tempColumnsCatalogHeader);
    }

    if (insertColumnsCatalogEntries(pColumnsCatalogFile, tempColumnsCatalogEntries, numOfAttrs, initialFreeSpaceOffset))
        return RM_INSERT_FAILED;

    // update the header of column catalog on dsik
    if (insertColumnsCatalogHeader(pColumnsCatalogFile, &tempColumnsCatalogHeader))
        return RM_INSERT_FAILED;

    
    // *************** Create RBF File ***************
    RecordBasedFileManager * _rbfm = RecordBasedFileManager::instance();
    if (_rbfm->createFile(tableName))
        return RBFM_CREATE_FAILED;

    // close the file
    fclose(pTablesCatalogFile);
    fclose(pColumnsCatalogFile);

    // TBC -- free _rbfm??

    return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName)
{
    /*
    // check if catalogs file exists
    if (!fileExists(tablesCatalogName))
        return FILE_DOES_NOT_EXIST;

    if (!fileExists(columnsCatalogName))
        return FILE_DOES_NOT_EXIST;

    // ******** DELETE RECORDS IN TABLES CATALOG ********
    // Read table catalog from disk
    FILE * pTablesFile = fopen(tablesCatalogName.c_str(), "rb+");
    TablesCatalogHeader tempTablesCatalogHeader;
    if (getTablesCatalogHeader(&tempTablesCatalogHeader, pTablesFile))
        return READ_FAILED;
    
    // Create temporary table catalog file to store the reamained records
    string newTablesCatalogName = tablesCatalogName + "_t";
    FILE * pNewTablesFile = fopen(newTablesCatalogName.c_str(), "wb");
    
    // Transfer the record to temorary table catalog
    uint32_t count = 0; // number of deleted records
    uint32_t tableId;
    string fileName;
    if (transferTablesCatalogRecoreds(pTablesFile, pNewTablesFile, tableName, sizeof(TablesCatalogHeader), 
                                tempTablesCatalogHeader.freeSpaceOffset, sizeof(TablesCatalogEntry), &count, &tableId, &fileName))
        return TRANSFER_FAILED;

    //Check if the RBF file exists
    if (count == 0) {
        fclose(pTablesFile);
        fclose(pNewTablesFile);
        remove(newTablesCatalogName.c_str());
        return FILE_DOES_NOT_EXIST;
    }

    // Insert the updated header to tables catalog
    tempTablesCatalogHeader.numOfRecords -= count;
    tempTablesCatalogHeader.freeSpaceOffset -= (count * sizeof(TablesCatalogEntry));
    insertTablesCatalogHeader(pNewTablesFile, &tempTablesCatalogHeader);

    // ******** DELETE RECORDS IN COLUMNS CATALOG ********
    // Read column catalog from disk 
    FILE * pColumnsFile = fopen(columnsCatalogName.c_str(), "rb+");
    ColumnsCatalogHeader tempColumnsCatalogHeader;
    if (getColumnsCatalogHeader(&tempColumnsCatalogHeader, pColumnsFile))
        return READ_FAILED;

    // Create temporary table catalog file to store the reamained records
    string newColumnsCatalogName = columnsCatalogName + "_t";
    FILE * pNewColumnsFile = fopen(newColumnsCatalogName.c_str(), "wb");

    // Transfer the record to temorary table catalog
    count = 0;
    if (transferColumnsCatalogRecoreds(pColumnsFile, pNewColumnsFile, tableId, sizeof(ColumnsCatalogHeader), tempColumnsCatalogHeader.freeSpaceOffset,
                                        sizeof(ColumnsCatalogEntry), &count))
        return TRANSFER_FAILED;

    // Insert the updated header to tables catalog
    tempColumnsCatalogHeader.freeSpaceOffset -= (count * sizeof(ColumnsCatalogEntry));
    insertColumnsCatalogHeader(pNewColumnsFile, &tempColumnsCatalogHeader);

    // close files
    fclose(pTablesFile);
    fclose(pColumnsFile);
    fclose(pNewTablesFile);
    fclose(pNewColumnsFile);

    // use temporary catalogs unstead of the original catalogs
    remove(tablesCatalogName.c_str());
    rename(newTablesCatalogName.c_str(), tablesCatalogName.c_str());

    remove(columnsCatalogName.c_str());
    rename(newColumnsCatalogName.c_str(), columnsCatalogName.c_str());

    // remove the RBF files
    remove(fileName.c_str());

    return SUCCESS;
    */
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    TablesCatalogEntry tempTablesCatalogEntry;
    uint32_t tableId;
    string fileName;

   if (getTableInfoByTableName(tableName, &tempTablesCatalogEntry))
        return TABLE_DOES_NOT_EXIST;

    tableId = tempTablesCatalogEntry.tableId;
    fileName = tempTablesCatalogEntry.fileName;

    // Check if the RBF file exists
    if (!fileExists(fileName)) {
        return FILE_DOES_NOT_EXIST;
    }

    // check if columns catalog exist
    if (!fileExists(columnsCatalogName))
        return FILE_DOES_NOT_EXIST;

    // Read columns catalog from disk
    FILE * pColumnsFile = fopen(columnsCatalogName.c_str() , "rb");
    ColumnsCatalogHeader tempColumnsCatalogHeader;
    if (getColumnsCatalogHeader(&tempColumnsCatalogHeader, pColumnsFile))
        return READ_FAILED;

    // Read columns attribute
    ColumnsCatalogEntry tempColumnsCatalogEntry;
    uint32_t offset = sizeof(ColumnsCatalogHeader);
    Attribute tempAttr;
    while (offset < tempColumnsCatalogHeader.freeSpaceOffset)
    {
        // read attribute from columns catalog on disk
        if (getColumnsCatalogEntry(pColumnsFile, offset, &tempColumnsCatalogEntry))
            return READ_FAILED;

        // find the target attributes
        if (tableId == tempColumnsCatalogEntry.tableId)
        {
            tempAttr.name = tempColumnsCatalogEntry.columnName;
            tempAttr.type = tempColumnsCatalogEntry.columnType;
            tempAttr.length = tempColumnsCatalogEntry.columnLength;
            attrs.push_back(tempAttr);
        }

        offset += sizeof(ColumnsCatalogEntry);
    }

    // close files
    fclose(pColumnsFile);

    return SUCCESS;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    TablesCatalogEntry tempTablesCatalogEntry;
    string fileName;
    
    // check if the table and the file of table exist
    if (getTableInfoByTableName(tableName, &tempTablesCatalogEntry))
        return TABLE_DOES_NOT_EXIST;

    fileName = tempTablesCatalogEntry.fileName;
    if (!fileExists(fileName))
        return FILE_DOES_NOT_EXIST;

    // open table file
    RecordBasedFileManager * _rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    _rbfm->openFile(fileName, fileHandle);
    
    // get tuple attributes from catalogs
    vector<Attribute> tupleAttrs;
    if (getAttributes(fileName, tupleAttrs))
        return READ_FAILED;

    // insert tuple
    if (_rbfm->insertRecord(fileHandle, tupleAttrs, data, rid))
        return INSERT_FAILED;

    // TBC -- free _rbfm??

    return SUCCESS;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    TablesCatalogEntry tempTablesCatalogEntry;
    string fileName;
    
    // check if the table and the file of table exist
    if (getTableInfoByTableName(tableName, &tempTablesCatalogEntry))
        return TABLE_DOES_NOT_EXIST;

    fileName = tempTablesCatalogEntry.fileName;
    if (!fileExists(fileName))
        return FILE_DOES_NOT_EXIST;

    // open table file
    RecordBasedFileManager * _rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    _rbfm->openFile(fileName, fileHandle);
    
    // get tuple attributes from catalogs
    vector<Attribute> tupleAttrs;
    if (getAttributes(fileName, tupleAttrs))
        return READ_FAILED;

    // delete tuple
    if (_rbfm->deleteRecord(fileHandle, tupleAttrs, rid))
        return DELETE_FAILED;

    // TBC -- free _rbfm??

    return SUCCESS;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    TablesCatalogEntry tempTablesCatalogEntry;
    string fileName;
    
    // check if the table and the file of table exist
    if (getTableInfoByTableName(tableName, &tempTablesCatalogEntry))
        return TABLE_DOES_NOT_EXIST;

    fileName = tempTablesCatalogEntry.fileName;
    if (!fileExists(fileName))
        return FILE_DOES_NOT_EXIST;

    // open table file
    RecordBasedFileManager * _rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    _rbfm->openFile(fileName, fileHandle);
    
    // get tuple attributes from catalogs
    vector<Attribute> tupleAttrs;
    if (getAttributes(fileName, tupleAttrs))
        return READ_FAILED;

    // delete tuple
    if (_rbfm->updateRecord(fileHandle, tupleAttrs, data, rid))
        return UPDATE_FAILED;

    // TBC -- free _rbfm??

    return SUCCESS;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    TablesCatalogEntry tempTablesCatalogEntry;
    string fileName;
    
    // check if the table and the file of table exist
    if (getTableInfoByTableName(tableName, &tempTablesCatalogEntry))
        return TABLE_DOES_NOT_EXIST;

    fileName = tempTablesCatalogEntry.fileName;
    if (!fileExists(fileName))
        return FILE_DOES_NOT_EXIST;

    // open table file
    RecordBasedFileManager * _rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    _rbfm->openFile(fileName, fileHandle);
    
    // get tuple attributes from catalogs
    vector<Attribute> tupleAttrs;
    if (getAttributes(fileName, tupleAttrs))
        return READ_FAILED;

    // insert tuple
    if (_rbfm->readRecord(fileHandle, tupleAttrs, rid, data))
        return READ_FAILED;

    // TBC -- free _rbfm??

    return SUCCESS;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	RecordBasedFileManager * _rbfm = RecordBasedFileManager::instance();
    _rbfm->printRecord(attrs, data);

    // TBC -- free _rbfm??

    return SUCCESS;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    TablesCatalogEntry tempTablesCatalogEntry;
    string fileName;
    
    // check if the table and the file of table exist
    if (getTableInfoByTableName(tableName, &tempTablesCatalogEntry))
        return TABLE_DOES_NOT_EXIST;

    fileName = tempTablesCatalogEntry.fileName;
    if (!fileExists(fileName))
        return FILE_DOES_NOT_EXIST;

    // open table file
    RecordBasedFileManager * _rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    _rbfm->openFile(fileName, fileHandle);
    
    // get tuple attributes from catalogs
    vector<Attribute> tupleAttrs;
    if (getAttributes(fileName, tupleAttrs))
        return READ_FAILED;

    // insert tuple
    if (_rbfm->readAttribute(fileHandle, tupleAttrs, rid, attributeName, data))
        return READ_FAILED;

    // TBC -- free _rbfm??

    return SUCCESS;
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
    return (stat(filename.c_str(), &sb) == 0);
}

RC RelationManager::getTableInfoByTableName(string tableName, TablesCatalogEntry * tablesCatalogEntry)
{
    // Check if catalog files exist
    if (!fileExists(tablesCatalogName))
        return FILE_DOES_NOT_EXIST;
        
    // get file name from tables catalog
    TablesCatalogHeader tempTablesCatalogHeader;
    FILE * pTablesFile = fopen(tablesCatalogName.c_str(), "rb");

    if (getTablesCatalogHeader(&tempTablesCatalogHeader, pTablesFile))
        return READ_FAILED;

    uint32_t offset = sizeof(TablesCatalogHeader);
    TablesCatalogEntry tempTablesCatalogEntry;

    while (offset < tempTablesCatalogHeader.freeSpaceOffset)
    {
        if (getTablesCatalogEntry(pTablesFile, offset, &tempTablesCatalogEntry))
            return READ_FAILED;
        
        if ((tableName.compare(tempTablesCatalogEntry.tableName)) == 0)
        {
            tablesCatalogEntry->tableId = tempTablesCatalogEntry.tableId;
            strcpy(tablesCatalogEntry->tableName, tableName.c_str());
            strcpy(tablesCatalogEntry->fileName, tempTablesCatalogEntry.fileName);
//            tablesCatalogEntry->fileName = tempTablesCatalogEntry.fileName;
            fclose(pTablesFile);
            return SUCCESS;
        } 
        offset += sizeof(TablesCatalogEntry);
    }

    fclose(pTablesFile);
    return TABLE_DOES_NOT_EXIST;
}

RC RelationManager::isTableNameExistInCatalog(string tableName, FILE * tablesCatalogFile, uint32_t numOfRecords)
{
    uint32_t offset;
    TablesCatalogEntry tempTablesCatalogEntry;
    
    for (uint32_t i = 0; i < numOfRecords; i++)
    {
        offset = sizeof(TablesCatalogHeader) + (i * sizeof(TablesCatalogEntry));
        if (getTablesCatalogEntry(tablesCatalogFile, offset, &tempTablesCatalogEntry))
            return READ_FAILED;

        // A table with the same name exist
        if (tableName.compare(tempTablesCatalogEntry.tableName) == 0)
            return RM_FILE_ALREADY_EXIST;
    }

    return SUCCESS;
}

void RelationManager::updateTablesCatalogEntry(TablesCatalogEntry * tablesCatalogEntry, uint32_t tableId, string tableName, string fileName)
{
	tablesCatalogEntry->tableId = tableId;
	strcpy(tablesCatalogEntry->tableName, tableName.c_str());
	strcpy(tablesCatalogEntry->fileName,fileName.c_str());
}

RC RelationManager::insertTablesCatalogEntries(FILE * pTablesFile, TablesCatalogEntry * tablesCatalogEntries, uint32_t numOfEntry)
{
    // It needs at least one entry to inert 
    if (numOfEntry < 1)
        return RM_INCORRECT_INPUT;

    uint32_t offset;

    // write each record to tables catalog on disk
    for(uint32_t i = 0; i < numOfEntry; i++)
    {
        offset = sizeof(TablesCatalogHeader) + ((tablesCatalogEntries[i].tableId - 1) * sizeof(TablesCatalogEntry));

        if (fseek(pTablesFile, offset, SEEK_SET))
            return RM_SEEK_FAILED;

        if (fwrite(&tablesCatalogEntries[i], sizeof(TablesCatalogEntry), 1, pTablesFile) != 1)
        {
            return RM_WRITE_FAILED;
        }

        fflush(pTablesFile);
    }

    return SUCCESS;
}

RC RelationManager::getTablesCatalogEntry(FILE * pTablesFile, uint32_t recordOffset, TablesCatalogEntry * tablesCatalogEntry)
{
    void * raw = malloc(sizeof(TablesCatalogEntry));

    fseek(pTablesFile, recordOffset, SEEK_SET);
    if (fread(raw, sizeof(TablesCatalogEntry), 1, pTablesFile) != 1)
        return READ_FAILED;

    memcpy(
        tablesCatalogEntry,
        raw,
        sizeof(TablesCatalogEntry)
    );

    free(raw);
    return SUCCESS;
}

void RelationManager::initializeTablesCatalogHeader(TablesCatalogHeader * tablesCatalogHeader)
{
    tablesCatalogHeader->nextTableId = 1;
    tablesCatalogHeader->numOfRecords = 0;
    tablesCatalogHeader->freeSpaceOffset = sizeof(TablesCatalogHeader);
}

void RelationManager::increaseTablesCatalogHeader(TablesCatalogHeader * tablescatalogHeader)
{
	tablescatalogHeader->nextTableId += 1;
	tablescatalogHeader->numOfRecords += 1;
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
    strcpy(columnsCatalogEntry->columnName,columnName.c_str());
    columnsCatalogEntry->columnType = columnType;
    columnsCatalogEntry->columnLength = columnLength;
    columnsCatalogEntry->columnPosition = columnPosition;
 }

RC RelationManager::insertColumnsCatalogEntries(FILE * pColumnsFile, ColumnsCatalogEntry * columnsCatalogEntries, uint32_t numOfEntry, uint32_t freeSpaceOffset)
{
    // It needs at least one entry to inert 
    if (numOfEntry < 1)
        return RM_INCORRECT_INPUT;

    uint32_t offset = freeSpaceOffset;

    // write each record to tables catalog on disk
    for(uint32_t i = 0; i < numOfEntry; i++)
    {
        if (fseek(pColumnsFile, offset, SEEK_SET))
            return RM_SEEK_FAILED;

        if (fwrite(&columnsCatalogEntries[i], sizeof(ColumnsCatalogEntry), 1, pColumnsFile) != 1)
        {
            return RM_WRITE_FAILED;
        }

        fflush(pColumnsFile);

        offset += sizeof(ColumnsCatalogEntry);
    }

    return SUCCESS;
}

RC RelationManager::getColumnsCatalogEntry(FILE * pColumnsFile, uint32_t recordOffset, ColumnsCatalogEntry * columnsCatalogEntry)
{
    void * raw = malloc(sizeof(ColumnsCatalogEntry));

    fseek(pColumnsFile, recordOffset, SEEK_SET);
    if (fread(raw, sizeof(ColumnsCatalogEntry), 1, pColumnsFile) != 1)
        return READ_FAILED;

    memcpy(
        columnsCatalogEntry,
        raw,
        sizeof(ColumnsCatalogEntry)
    );

    free(raw);
    return SUCCESS;
}


void RelationManager::initializeColumnsCatalogHeader(ColumnsCatalogHeader * columnsCatalogHeader)
{
    columnsCatalogHeader->freeSpaceOffset = sizeof(ColumnsCatalogHeader);
    columnsCatalogHeader->numOfRecords = 0;
}


void RelationManager::increaseColumnsCatalogHeader(ColumnsCatalogHeader * columnsCatalogHeader)
{
	columnsCatalogHeader->freeSpaceOffset += sizeof(ColumnsCatalogEntry);
    columnsCatalogHeader->numOfRecords += 1;
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

//  TBD -- Testing Function 
void RelationManager::printTableCatalog() {
    TablesCatalogEntry tempEntry;
    TablesCatalogHeader tempHeader;
    void * rawHeader = malloc(sizeof(TablesCatalogHeader));
    void * rawEntry = malloc(sizeof(TablesCatalogEntry));

    FILE * fTableCatalog = fopen(tablesCatalogName.c_str(), "rb");
    rewind(fTableCatalog);
    fread(rawHeader, sizeof(TablesCatalogHeader), 1, fTableCatalog);
    memcpy(&tempHeader, rawHeader, sizeof(TablesCatalogHeader));

    cout << "===== Header =====" << endl
    << tempHeader.nextTableId << endl
    << tempHeader.numOfRecords << endl
    << tempHeader.freeSpaceOffset << endl
    << "MOD:" << (tempHeader.freeSpaceOffset - sizeof(TablesCatalogHeader)) % sizeof(TablesCatalogEntry) << endl;

    uint32_t offset = sizeof(TablesCatalogHeader);
    cout << "===== Entry =====" << endl;
    while (offset < tempHeader.freeSpaceOffset) {
        fseek(fTableCatalog, offset, SEEK_SET);
        fread(rawEntry, sizeof(TablesCatalogEntry), 1, fTableCatalog);
        memcpy(&tempEntry, rawEntry, sizeof(TablesCatalogEntry));

        cout << tempEntry.tableId << endl
        << tempEntry.tableName << endl
        << tempEntry.fileName << endl;
        
        offset += sizeof(TablesCatalogEntry);
        cout << "MOD: " << (tempHeader.freeSpaceOffset - offset) % sizeof(TablesCatalogEntry) << endl;
        
        cout << "----- next -----" << endl;
    }

}

void RelationManager::printColumnsCatalog() {
    ColumnsCatalogEntry tempEntry;
    ColumnsCatalogHeader tempHeader;
    void * rawHeader = malloc(sizeof(ColumnsCatalogHeader));
    void * rawEntry = malloc(sizeof(ColumnsCatalogEntry));

    FILE * fColumnCatalog = fopen(columnsCatalogName.c_str(), "rb");
    rewind(fColumnCatalog);
    fread(rawHeader, sizeof(ColumnsCatalogHeader), 1, fColumnCatalog);
    memcpy(&tempHeader, rawHeader, sizeof(ColumnsCatalogHeader));

    cout << "===== Header =====" << endl
    << (tempHeader.freeSpaceOffset - sizeof(ColumnsCatalogHeader)) % sizeof(ColumnsCatalogEntry) << endl
    << tempHeader.freeSpaceOffset << endl
    << "MOD:" << (tempHeader.freeSpaceOffset - sizeof(ColumnsCatalogHeader)) % sizeof(ColumnsCatalogEntry) << endl;
;

    uint32_t offset = sizeof(ColumnsCatalogHeader);
    cout << "===== Entry =====" << endl;
    while (offset < tempHeader.freeSpaceOffset) {
        fseek(fColumnCatalog, offset, SEEK_SET);
        fread(rawEntry, sizeof(ColumnsCatalogEntry), 1, fColumnCatalog);
        memcpy(&tempEntry, rawEntry, sizeof(ColumnsCatalogEntry));

        cout << tempEntry.tableId << endl
        << tempEntry.columnName << endl
        << tempEntry.columnType << endl
        << tempEntry.columnLength << endl
        << tempEntry.columnPosition << endl;
        
        offset += sizeof(ColumnsCatalogEntry);
        cout << "MOD:" << (tempHeader.freeSpaceOffset - sizeof(ColumnsCatalogHeader)) % sizeof(ColumnsCatalogEntry) << endl;

        cout << "----- next -----" << endl;
    }
}
