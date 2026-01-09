/*
 * Copyright (c) 2026 Firefly Software Solutions Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
Metadata Provider Implementation
================================

This file implements the MetadataProvider interface for ODBC/JDBC driver support.
It provides database metadata such as tables, columns, primary keys, foreign keys,
and indexes.

The metadata provider uses the SQL catalog to retrieve schema information and
formats it in a way that's compatible with ODBC/JDBC metadata APIs.
*/
package server

import (
	"flydb/internal/sql"
	"strings"
)

// serverMetadataProvider implements the protocol.MetadataProvider interface.
type serverMetadataProvider struct {
	srv *Server
}

// GetTables returns table metadata matching the specified pattern.
func (m *serverMetadataProvider) GetTables(catalog, schema, tablePattern string, tableTypes []string) ([][]interface{}, error) {
	executor := m.srv.executor
	if executor == nil {
		return nil, nil
	}

	// Get catalog from executor
	cat := executor.GetCatalog()
	if cat == nil {
		return nil, nil
	}

	var rows [][]interface{}
	for tableName, tableSchema := range cat.Tables {
		// Apply pattern matching
		if tablePattern != "" && tablePattern != "%" && !matchPattern(tableName, tablePattern) {
			continue
		}

		// Determine table type
		tableType := "TABLE"
		if _, isView := cat.GetView(tableName); isView {
			tableType = "VIEW"
		}

		// Filter by table types if specified
		if len(tableTypes) > 0 {
			found := false
			for _, t := range tableTypes {
				if strings.EqualFold(t, tableType) {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		row := []interface{}{
			catalog,                // TABLE_CAT
			schema,                 // TABLE_SCHEM
			tableName,              // TABLE_NAME
			tableType,              // TABLE_TYPE
			"",                     // REMARKS (TableSchema doesn't have Comment field)
		}
		// Suppress unused variable warning
		_ = tableSchema
		rows = append(rows, row)
	}

	return rows, nil
}

// GetColumns returns column metadata for the specified table.
func (m *serverMetadataProvider) GetColumns(catalog, schema, tablePattern, columnPattern string) ([][]interface{}, error) {
	executor := m.srv.executor
	if executor == nil {
		return nil, nil
	}

	cat := executor.GetCatalog()
	if cat == nil {
		return nil, nil
	}

	var rows [][]interface{}
	for tableName, tableSchema := range cat.Tables {
		// Apply table pattern matching
		if tablePattern != "" && tablePattern != "%" && !matchPattern(tableName, tablePattern) {
			continue
		}

		for i, col := range tableSchema.Columns {
			// Apply column pattern matching
			if columnPattern != "" && columnPattern != "%" && !matchPattern(col.Name, columnPattern) {
				continue
			}

			nullable := 1 // SQL_NULLABLE
			isNullable := "YES"
			if col.IsNotNull() {
				nullable = 0 // SQL_NO_NULLS
				isNullable = "NO"
			}

			// Get column size based on type
			colSize := getColumnSize(col.Type)

			row := []interface{}{
				catalog,              // TABLE_CAT
				schema,               // TABLE_SCHEM
				tableName,            // TABLE_NAME
				col.Name,             // COLUMN_NAME
				getSQLType(col.Type), // DATA_TYPE
				col.Type,             // TYPE_NAME
				colSize,              // COLUMN_SIZE
				0,                    // BUFFER_LENGTH
				0,                    // DECIMAL_DIGITS
				10,                   // NUM_PREC_RADIX
				nullable,             // NULLABLE
				"",                   // REMARKS
				getDefaultValue(col), // COLUMN_DEF
				getSQLType(col.Type), // SQL_DATA_TYPE
				0,                    // SQL_DATETIME_SUB
				colSize,              // CHAR_OCTET_LENGTH
				i + 1,                // ORDINAL_POSITION
				isNullable,           // IS_NULLABLE
			}
			rows = append(rows, row)
		}
	}

	return rows, nil
}

// GetPrimaryKeys returns primary key information for the specified table.
func (m *serverMetadataProvider) GetPrimaryKeys(catalog, schema, table string) ([][]interface{}, error) {
	executor := m.srv.executor
	if executor == nil {
		return nil, nil
	}

	cat := executor.GetCatalog()
	if cat == nil {
		return nil, nil
	}

	tableSchema, ok := cat.Tables[table]
	if !ok {
		return nil, nil
	}

	pkCols := tableSchema.GetPrimaryKeyColumns()
	var rows [][]interface{}
	for i, colName := range pkCols {
		row := []interface{}{
			catalog,                    // TABLE_CAT
			schema,                     // TABLE_SCHEM
			table,                      // TABLE_NAME
			colName,                    // COLUMN_NAME
			i + 1,                      // KEY_SEQ
			table + "_pkey",            // PK_NAME
		}
		rows = append(rows, row)
	}

	return rows, nil
}

// GetForeignKeys returns foreign key information for the specified table.
func (m *serverMetadataProvider) GetForeignKeys(catalog, schema, table string) ([][]interface{}, error) {
	executor := m.srv.executor
	if executor == nil {
		return nil, nil
	}

	cat := executor.GetCatalog()
	if cat == nil {
		return nil, nil
	}

	tableSchema, ok := cat.Tables[table]
	if !ok {
		return nil, nil
	}

	fks := tableSchema.GetForeignKeys()
	var rows [][]interface{}
	for i, fk := range fks {
		row := []interface{}{
			catalog,                    // PKTABLE_CAT
			schema,                     // PKTABLE_SCHEM
			fk.RefTable,                // PKTABLE_NAME
			fk.RefColumn,               // PKCOLUMN_NAME
			catalog,                    // FKTABLE_CAT
			schema,                     // FKTABLE_SCHEM
			table,                      // FKTABLE_NAME
			fk.Column,                  // FKCOLUMN_NAME
			i + 1,                      // KEY_SEQ
			1,                          // UPDATE_RULE (CASCADE)
			1,                          // DELETE_RULE (CASCADE)
			table + "_" + fk.Column + "_fkey", // FK_NAME
			fk.RefTable + "_pkey",      // PK_NAME
			7,                          // DEFERRABILITY (NOT DEFERRABLE)
		}
		rows = append(rows, row)
	}

	return rows, nil
}

// GetIndexes returns index information for the specified table.
func (m *serverMetadataProvider) GetIndexes(catalog, schema, table string, unique bool) ([][]interface{}, error) {
	executor := m.srv.executor
	if executor == nil {
		return nil, nil
	}

	indexMgr := executor.GetIndexManager()
	if indexMgr == nil {
		return nil, nil
	}

	columns := indexMgr.GetIndexedColumns(table)
	var rows [][]interface{}
	for i, colName := range columns {
		// For now, all indexes are non-unique B-tree indexes
		// Skip if unique filter is set and index is not unique
		if unique {
			continue // We don't track unique indexes separately yet
		}

		row := []interface{}{
			catalog,                    // TABLE_CAT
			schema,                     // TABLE_SCHEM
			table,                      // TABLE_NAME
			true,                       // NON_UNIQUE
			catalog,                    // INDEX_QUALIFIER
			"idx_" + table + "_" + colName, // INDEX_NAME
			3,                          // TYPE (SQL_INDEX_OTHER)
			i + 1,                      // ORDINAL_POSITION
			colName,                    // COLUMN_NAME
			"A",                        // ASC_OR_DESC
			0,                          // CARDINALITY
			0,                          // PAGES
			nil,                        // FILTER_CONDITION
		}
		rows = append(rows, row)
	}

	return rows, nil
}

// GetTypeInfo returns information about supported data types.
func (m *serverMetadataProvider) GetTypeInfo() ([][]interface{}, error) {
	// Return standard SQL types supported by FlyDB
	types := [][]interface{}{
		{"INTEGER", 4, 10, nil, nil, nil, 1, false, 3, false, false, false, "INTEGER", 0, 0, 4, nil, 10},
		{"BIGINT", -5, 19, nil, nil, nil, 1, false, 3, false, false, false, "BIGINT", 0, 0, -5, nil, 10},
		{"SMALLINT", 5, 5, nil, nil, nil, 1, false, 3, false, false, false, "SMALLINT", 0, 0, 5, nil, 10},
		{"REAL", 7, 7, nil, nil, nil, 1, false, 3, false, false, false, "REAL", 0, 0, 7, nil, 10},
		{"DOUBLE", 8, 15, nil, nil, nil, 1, false, 3, false, false, false, "DOUBLE", 0, 0, 8, nil, 10},
		{"VARCHAR", 12, 65535, "'", "'", "length", 1, true, 3, false, false, false, "VARCHAR", 0, 0, 12, nil, nil},
		{"TEXT", -1, 2147483647, "'", "'", nil, 1, true, 3, false, false, false, "TEXT", 0, 0, -1, nil, nil},
		{"BOOLEAN", -7, 1, nil, nil, nil, 1, false, 3, false, false, false, "BOOLEAN", 0, 0, -7, nil, nil},
		{"DATE", 91, 10, "'", "'", nil, 1, false, 3, false, false, false, "DATE", 0, 0, 9, 1, nil},
		{"TIME", 92, 8, "'", "'", nil, 1, false, 3, false, false, false, "TIME", 0, 0, 9, 2, nil},
		{"TIMESTAMP", 93, 26, "'", "'", nil, 1, false, 3, false, false, false, "TIMESTAMP", 0, 0, 9, 3, nil},
		{"BLOB", -4, 2147483647, nil, nil, nil, 1, false, 0, false, false, false, "BLOB", 0, 0, -4, nil, nil},
	}
	return types, nil
}

// getSQLType returns the ODBC SQL type code for a column type.
func getSQLType(typeName string) int {
	switch strings.ToUpper(typeName) {
	case "INT", "INTEGER":
		return 4 // SQL_INTEGER
	case "BIGINT":
		return -5 // SQL_BIGINT
	case "SMALLINT":
		return 5 // SQL_SMALLINT
	case "REAL", "FLOAT":
		return 7 // SQL_REAL
	case "DOUBLE", "DOUBLE PRECISION":
		return 8 // SQL_DOUBLE
	case "VARCHAR", "CHAR", "CHARACTER":
		return 12 // SQL_VARCHAR
	case "TEXT":
		return -1 // SQL_LONGVARCHAR
	case "BOOLEAN", "BOOL":
		return -7 // SQL_BIT
	case "DATE":
		return 91 // SQL_TYPE_DATE
	case "TIME":
		return 92 // SQL_TYPE_TIME
	case "TIMESTAMP", "DATETIME":
		return 93 // SQL_TYPE_TIMESTAMP
	case "BLOB", "BINARY":
		return -4 // SQL_LONGVARBINARY
	default:
		return 12 // Default to VARCHAR
	}
}

// matchPattern matches a string against a SQL-like pattern with % wildcards.
func matchPattern(s, pattern string) bool {
	if pattern == "" || pattern == "%" {
		return true
	}
	// Simple pattern matching - supports % at start/end
	pattern = strings.ToLower(pattern)
	s = strings.ToLower(s)

	if strings.HasPrefix(pattern, "%") && strings.HasSuffix(pattern, "%") {
		return strings.Contains(s, pattern[1:len(pattern)-1])
	}
	if strings.HasPrefix(pattern, "%") {
		return strings.HasSuffix(s, pattern[1:])
	}
	if strings.HasSuffix(pattern, "%") {
		return strings.HasPrefix(s, pattern[:len(pattern)-1])
	}
	return s == pattern
}

// getColumnSize returns the default size for a column type.
func getColumnSize(typeName string) int {
	switch strings.ToUpper(typeName) {
	case "INT", "INTEGER":
		return 10
	case "BIGINT":
		return 19
	case "SMALLINT":
		return 5
	case "REAL", "FLOAT":
		return 7
	case "DOUBLE", "DOUBLE PRECISION":
		return 15
	case "VARCHAR", "CHAR", "CHARACTER":
		return 255
	case "TEXT":
		return 65535
	case "BOOLEAN", "BOOL":
		return 1
	case "DATE":
		return 10
	case "TIME":
		return 8
	case "TIMESTAMP", "DATETIME":
		return 26
	case "BLOB", "BINARY":
		return 65535
	default:
		return 255
	}
}

// getDefaultValue extracts the default value from a column definition.
func getDefaultValue(col sql.ColumnDef) interface{} {
	for _, constraint := range col.Constraints {
		if constraint.Type == sql.ConstraintDefault {
			return constraint.DefaultValue
		}
	}
	return nil
}

