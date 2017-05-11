
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
    updateColumnsCatalogEntry(&columnsCatalogEntry, 0, "table-id", TypeInt, 4 , 1);
    if (insertColumnsCatalogEntry(pColumnsFile, &columnsCatalogEntry, &columnsCatalogHeader))
        return INSERT_FAILED;
    updateColumnsCatalogHeader(&columnsCatalogHeader);

    updateColumnsCatalogEntry(&columnsCatalogEntry, 0, "table-name", TypeVarChar, 50, 2);
    if (insertColumnsCatalogEntry(pColumnsFile, &columnsCatalogEntry, &columnsCatalogHeader))
        return INSERT_FAILED;
    updateColumnsCatalogHeader(&columnsCatalogHeader);

    updateColumnsCatalogEntry(&columnsCatalogEntry, 0, "file-name", TypeVarChar, 50, 3);
    if (insertColumnsCatalogEntry(pColumnsFile, &columnsCatalogEntry, &columnsCatalogHeader))
        return INSERT_FAILED;
    updateColumnsCatalogHeader(&columnsCatalogHeader);

    updateColumnsCatalogEntry(&columnsCatalogEntry, 1, "table-id", TypeInt, 4, 1);
    if (insertColumnsCatalogEntry(pColumnsFile, &columnsCatalogEntry, &columnsCatalogHeader))
        return INSERT_FAILED;
    updateColumnsCatalogHeader(&columnsCatalogHeader);

    updateColumnsCatalogEntry(&columnsCatalogEntry, 1, "column-name", TypeVarChar, 50, 2);
    if (insertColumnsCatalogEntry(pColumnsFile, &columnsCatalogEntry, &columnsCatalogHeader))
        return INSERT_FAILED;
    updateColumnsCatalogHeader(&columnsCatalogHeader);

    updateColumnsCatalogEntry(&columnsCatalogEntry, 1, "column-type", TypeInt, 4, 3);
    if (insertColumnsCatalogEntry(pColumnsFile, &columnsCatalogEntry, &columnsCatalogHeader))
        return INSERT_FAILED;
    updateColumnsCatalogHeader(&columnsCatalogHeader);

    updateColumnsCatalogEntry(&columnsCatalogEntry, 1, "column-length", TypeInt, 4, 4);
    if (insertColumnsCatalogEntry(pColumnsFile, &columnsCatalogEntry, &columnsCatalogHeader))
        return INSERT_FAILED;
    updateColumnsCatalogHeader(&columnsCatalogHeader);

    updateColumnsCatalogEntry(&columnsCatalogEntry, 1, "column-position", TypeInt, 4, 5);
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
    if (!fileExists(tablesCatalogName))
        return FILE_DOES_NOT_EXIST;

    if (!fileExists(columnsCatalogName))
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
    if (!fileExists(tablesCatalogName))
        return FILE_DOES_NOT_EXIST;

    if (!fileExists(columnsCatalogName))
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
    string columnName;
    AttrType columnType;
    uint32_t columnLength;

    // insert each columns attribute to disk
    for(uint16_t i = 0; i < attrs.size(); i++)
    {
        columnName = attrs[i].name;
        columnType = attrs[i].type;
        columnLength = attrs[i].length;
        updateColumnsCatalogEntry(&tempColumnsCatalogEntry, tableId, columnName,
                                    columnType, columnLength, i);
        if (insertColumnsCatalogEntry(pColumnsFile, &tempColumnsCatalogEntry, &tempColumnsCatalogHeader))
            return INSERT_FAILED;
        
        // udpate the column catelog header in memory
        updateColumnsCatalogHeader(&tempColumnsCatalogHeader);
    }

    // update the header of column catalog on dsik
    insertColumnsCatalogHeader(pColumnsFile, &tempColumnsCatalogHeader);

    // close the file
    fclose(pTablesFile);
    fclose(pColumnsFile);

    return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName)
{
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
    tempTablesCatalogHeader.numOfTables -= count;
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
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    // check if catalogs file exists
    if (!fileExists(tablesCatalogName))
        return FILE_DOES_NOT_EXIST;

    if (!fileExists(columnsCatalogName))
        return FILE_DOES_NOT_EXIST;

    // Read table catalog from disk
    FILE * pTablesFile = fopen(tablesCatalogName.c_str(), "rb");
    TablesCatalogHeader tempTablesCatalogHeader;
    if (getTablesCatalogHeader(&tempTablesCatalogHeader, pTablesFile))
        return READ_FAILED;
    
    // Get tableId and fileName from the catalogs on disk
    uint32_t count = 0;                             // number of deleted records
    uint32_t offset = sizeof(TablesCatalogHeader);  // offset start from the beginning of the first record in tables catalog
    uint32_t tableId;
    string fileName;
    TablesCatalogEntry tempTablesCatalogEntry;
    
    while (offset < tempTablesCatalogHeader.freeSpaceOffset){
        // record record from the file on disk
        if (getTablesCatalogEntry(pTablesFile, offset, &tempTablesCatalogEntry))
            return READ_FAILED;
        
        // get the the record id and file name that corresponding to the table
        if (tableName.compare(tempTablesCatalogEntry.tableName) == 0)
        {
            count += 1;
            tableId = tempTablesCatalogEntry.tableId;
            fileName = tempTablesCatalogEntry.fileName;
            break;
        }

        offset += sizeof(TablesCatalogEntry);
    }

    // Check if the RBF file exists
    if (count == 0) {
        fclose(pTablesFile);
        return FILE_DOES_NOT_EXIST;
    }

    // Read columns catalog from disk
    FILE * pColumnsFile = fopen(columnsCatalogName.c_str() , "rb");
    ColumnsCatalogHeader tempColumnsCatalogHeader;
    if (getColumnsCatalogHeader(&tempColumnsCatalogHeader, pColumnsFile))
        return READ_FAILED;

    // Read columns attribute
    ColumnsCatalogEntry tempColumnsCatalogEntry;
    Attribute tempAttr;
    offset = sizeof(ColumnsCatalogHeader);
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

    return SUCCESS;
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
    return (stat(filename.c_str(), &sb) == 0);
}

RC RelationManager::transferTablesCatalogRecoreds(FILE * srcFile, FILE * destFile, string tableName, uint32_t headOffset,
                                                 uint32_t endOffset, uint32_t sizeOfRecord, uint32_t * count, uint32_t * tableId, string * fileName)
{
    // no record in the source file
    if (headOffset == endOffset)
        return SUCCESS;

    uint32_t currentOffset = headOffset;
    uint32_t newFreeSpaceOffset = headOffset;
    void * raw = malloc(sizeOfRecord);
    TablesCatalogEntry tempTablesCatalogEntry;

    while (currentOffset < endOffset) {
        // get record fom the disk
        if (getTablesCatalogEntry(srcFile, currentOffset, &tempTablesCatalogEntry))
            return READ_FAILED;

        // write the record to destination file if it is not the target record
        if (tableName.compare(tempTablesCatalogEntry.tableName) != 0)
        {
            fseek(destFile, newFreeSpaceOffset, SEEK_SET);
            if (fwrite(&tempTablesCatalogEntry, sizeOfRecord, 1, destFile) != 1)
                return WRITE_FAILED;
            fflush(destFile);
            newFreeSpaceOffset += sizeOfRecord;
        }
        else
        {
            *count += 1;
            *tableId = tempTablesCatalogEntry.tableId;
            *fileName = tempTablesCatalogEntry.fileName;
        }

        currentOffset += sizeOfRecord;
    }

    free(raw);
    return SUCCESS;
}

RC RelationManager::transferColumnsCatalogRecoreds(FILE * srcFile, FILE * destFile, uint32_t tableId, uint32_t headOffset, uint32_t endOffset, uint32_t sizeOfRecord, uint32_t * count)
{
    // no record in the source file
    if (headOffset == endOffset)
        return SUCCESS;

    uint32_t currentOffset = headOffset;
    uint32_t newFreeSpaceOffset = headOffset;
    void * raw = malloc(sizeOfRecord);
    ColumnsCatalogEntry tempColumnsCatalogEntry;
    while (currentOffset < endOffset) {
        // get record fom the disk
        if (getColumnsCatalogEntry(srcFile, currentOffset, &tempColumnsCatalogEntry))
            return READ_FAILED;

        // write the record to destination file if it is not the target record
        if (tableId != tempColumnsCatalogEntry.tableId)
        {
            fseek(destFile, newFreeSpaceOffset, SEEK_SET);
            if (fwrite(&tempColumnsCatalogEntry, sizeOfRecord, 1, destFile) != 1)
                return WRITE_FAILED;
            fflush(destFile);
            newFreeSpaceOffset += sizeOfRecord;
        }
        else
        {
            *count += 1;
        }

        currentOffset += sizeOfRecord;
    }

    free(raw);
    return SUCCESS;
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

    if (fseek(pTablesFile, (sizeof(TablesCatalogHeader) + (tableId * sizeof(TablesCatalogEntry))), SEEK_SET))
        return SEEK_FAILED;

    if (fwrite(tablesCatalogEntry, sizeof(TablesCatalogEntry), 1, pTablesFile))
    {
        fflush(pTablesFile);
        return SUCCESS;
    }

    return WRITE_FAILED;
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
    << tempHeader.numOfTables << endl
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