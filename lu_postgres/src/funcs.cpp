/*
	PostgreSQL Functions for Squirrel
	Uses libpqxx C++ library for PostgreSQL connectivity
*/

#include "funcs.hpp"
#include <pqxx/pqxx>
#include <sdk/SQModule.h>
#include <sdk/squirrel.h>
#include <string.h>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <vector>
#include <memory>
#include <stdexcept>
#include <cctype>
#include <functional>

extern SQAPI sq;
extern HSQUIRRELVM v;

// Global storage for connections, transactions, and results
std::vector<std::unique_ptr<pqxx::connection>> g_connections;
std::vector<std::unique_ptr<pqxx::work>> g_transactions;

// Results are no longer stored globally - they're returned directly or cleaned up automatically

// Helper function to check if query is a SELECT statement
bool is_select_query(const char* query) {
	// Skip whitespace
	while (*query && isspace(*query)) query++;

	// Check if it starts with SELECT (case insensitive)
	const char* select_keyword = "SELECT";
	for (int i = 0; i < 6; i++) {
		if (toupper(query[i]) != select_keyword[i]) {
			return false;
		}
	}
	return true;
}

// Shared JSON parser for both automatic decoding and manual decode function
struct JSONParser {
	const char* str;
	size_t pos;
	HSQUIRRELVM vm;
	SQAPI sq;

	JSONParser(const char* s, HSQUIRRELVM v, SQAPI api) : str(s), pos(0), vm(v), sq(api) {}

	void skipWhitespace() {
		while (str[pos] && (str[pos] == ' ' || str[pos] == '\t' || str[pos] == '\n' || str[pos] == '\r')) {
			pos++;
		}
	}

	bool parseValue() {
		skipWhitespace();

		if (!str[pos]) return false;

		if (str[pos] == '{') return parseObject();
		if (str[pos] == '[') return parseArray();
		if (str[pos] == '"') return parseString();
		if (str[pos] == 't' || str[pos] == 'f') return parseBool();
		if (str[pos] == 'n') return parseNull();
		if (str[pos] == '-' || (str[pos] >= '0' && str[pos] <= '9')) return parseNumber();

		return false;
	}

	bool parseObject() {
		pos++; // skip '{'
		sq->newtable(vm);

		skipWhitespace();
		if (str[pos] == '}') {
			pos++;
			return true;
		}

		while (true) {
			skipWhitespace();

			// Parse key
			if (str[pos] != '"') return false;
			if (!parseString()) return false;

			skipWhitespace();
			if (str[pos] != ':') return false;
			pos++;

			// Parse value
			if (!parseValue()) return false;

			// Add to table
			sq->rawset(vm, -3);

			skipWhitespace();
			if (str[pos] == '}') {
				pos++;
				return true;
			}
			if (str[pos] != ',') return false;
			pos++;
		}
	}

	bool parseArray() {
		pos++; // skip '['
		sq->newarray(vm, 0);

		skipWhitespace();
		if (str[pos] == ']') {
			pos++;
			return true;
		}

		while (true) {
			if (!parseValue()) return false;
			sq->arrayappend(vm, -2);

			skipWhitespace();
			if (str[pos] == ']') {
				pos++;
				return true;
			}
			if (str[pos] != ',') return false;
			pos++;
		}
	}

	bool parseString() {
		pos++; // skip opening '"'
		std::string result;

		while (str[pos] && str[pos] != '"') {
			if (str[pos] == '\\') {
				pos++;
				if (!str[pos]) return false;

				switch (str[pos]) {
				case '"': result += '"'; break;
				case '\\': result += '\\'; break;
				case '/': result += '/'; break;
				case 'b': result += '\b'; break;
				case 'f': result += '\f'; break;
				case 'n': result += '\n'; break;
				case 'r': result += '\r'; break;
				case 't': result += '\t'; break;
				default: result += str[pos]; break;
				}
				pos++;
			}
			else {
				result += str[pos++];
			}
		}

		if (str[pos] != '"') return false;
		pos++; // skip closing '"'

		sq->pushstring(vm, result.c_str(), result.length());
		return true;
	}

	bool parseNumber() {
		size_t start = pos;
		bool is_float = false;

		if (str[pos] == '-') pos++;

		while (str[pos] >= '0' && str[pos] <= '9') pos++;

		if (str[pos] == '.') {
			is_float = true;
			pos++;
			while (str[pos] >= '0' && str[pos] <= '9') pos++;
		}

		if (str[pos] == 'e' || str[pos] == 'E') {
			is_float = true;
			pos++;
			if (str[pos] == '+' || str[pos] == '-') pos++;
			while (str[pos] >= '0' && str[pos] <= '9') pos++;
		}

		std::string num_str(str + start, pos - start);

		if (is_float) {
			sq->pushfloat(vm, std::stod(num_str));
		}
		else {
			sq->pushinteger(vm, std::stoll(num_str));
		}

		return true;
	}

	bool parseBool() {
		if (strncmp(str + pos, "true", 4) == 0) {
			sq->pushbool(vm, SQTrue);
			pos += 4;
			return true;
		}
		if (strncmp(str + pos, "false", 5) == 0) {
			sq->pushbool(vm, SQFalse);
			pos += 5;
			return true;
		}
		return false;
	}

	bool parseNull() {
		if (strncmp(str + pos, "null", 4) == 0) {
			sq->pushnull(vm);
			pos += 4;
			return true;
		}
		return false;
	}
};

// Helper function to add result rows to Squirrel table
void push_result_as_table(HSQUIRRELVM v, const pqxx::result& result) {
	sq->newtable(v);
	sq->pushstring(v, "rows", -1);
	sq->newarray(v, 0);

	// Add all rows
	for (size_t row_idx = 0; row_idx < result.size(); ++row_idx)
	{
		const auto& row = result[row_idx];
		sq->newtable(v);

		for (size_t col_idx = 0; col_idx < row.size(); ++col_idx)
		{
			std::string field_name = result.column_name(col_idx);
			sq->pushstring(v, field_name.c_str(), field_name.length());

			if (row[col_idx].is_null())
			{
				sq->pushnull(v);
			}
			else
			{
				const auto& field = row[col_idx];
				try {
					pqxx::oid type_oid = field.type();

					// JSON (114) and JSONB (3802) types - automatically decode
					if (type_oid == 114 || type_oid == 3802)
					{
						std::string json_str = field.c_str();

						// Use shared JSON parser
						JSONParser parser(json_str.c_str(), v, sq);
						if (!parser.parseValue()) {
							// If parsing fails, return as string
							sq->pushstring(v, json_str.c_str(), json_str.length());
						}
					}
					else if (type_oid == 21 || type_oid == 23 || type_oid == 20)
					{
						sq->pushinteger(v, field.as<long long>());
					}
					else if (type_oid == 700 || type_oid == 701 || type_oid == 1700)
					{
						sq->pushfloat(v, field.as<double>());
					}
					else if (type_oid == 16)
					{
						sq->pushbool(v, field.as<bool>() ? SQTrue : SQFalse);
					}
					else
					{
						std::string str_val = field.c_str();
						sq->pushstring(v, str_val.c_str(), str_val.length());
					}
				}
				catch (...)
				{
					std::string str_val = field.c_str();
					sq->pushstring(v, str_val.c_str(), str_val.length());
				}
			}

			sq->rawset(v, -3);
		}
		sq->arrayappend(v, -2);
	}

	sq->rawset(v, -3);

	// Add row count
	sq->pushstring(v, "count", -1);
	sq->pushinteger(v, (SQInteger)result.size());
	sq->rawset(v, -3);
}

// Connect to PostgreSQL database
// Usage: postgres_connect(connection_string)
// Returns: connection handle
// Throws: error on failure
_SQUIRRELDEF(SQ_postgres_connect)
{
	if (sq->gettype(v, 2) != OT_STRING)
	{
		return sq->throwerror(v, "Error in 'postgres_connect': Expected connection string");
	}

	const char* connstr;
	sq->getstring(v, 2, &connstr);

	try {
		auto& conn = g_connections.emplace_back(std::make_unique<pqxx::connection>(connstr));

		if (!conn->is_open())
		{
			g_connections.pop_back();
			return sq->throwerror(v, "Failed to open connection");
		}

		sq->pushuserpointer(v, conn.get());
		return 1;
	}
	catch (const std::exception& e) {
		return sq->throwerror(v, e.what());
	}
}

// Close PostgreSQL connection
// Usage: postgres_close(connection)
// Returns: true on success
_SQUIRRELDEF(SQ_postgres_close)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER)
	{
		return sq->throwerror(v, "Error in 'postgres_close': Expected connection handle");
	}

	void* ptr{};
	sq->getuserpointer(v, 2, &ptr);

	for (auto it = g_connections.begin(); it != g_connections.end(); ++it) {
		if (it->get() == ptr) {
			g_connections.erase(it);
			sq->pushbool(v, SQTrue);
			return 1;
		}
	}

	return sq->throwerror(v, "Error in 'postgres_close': Invalid connection handle");
}

// Check if connection is open
// Usage: postgres_is_open(connection)
// Returns: true if connection is open
_SQUIRRELDEF(SQ_postgres_is_open)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER)
	{
		return sq->throwerror(v, "Error in 'postgres_is_open': Expected connection handle");
	}

	void* ptr{};
	sq->getuserpointer(v, 2, &ptr);

	auto conn = static_cast<pqxx::connection*>(ptr);
	sq->pushbool(v, conn->is_open() ? SQTrue : SQFalse);
	return 1;
}

// Begin a new transaction
// Usage: postgres_begin(connection)
// Returns: transaction handle
// Throws: error on failure
_SQUIRRELDEF(SQ_postgres_begin)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER)
	{
		return sq->throwerror(v, "Error in 'postgres_begin': Expected connection handle");
	}

	void* ptr{};
	sq->getuserpointer(v, 2, &ptr);

	auto conn = static_cast<pqxx::connection*>(ptr);

	try {
		auto& txn = g_transactions.emplace_back(std::make_unique<pqxx::work>(*conn));
		sq->pushuserpointer(v, txn.get());
		return 1;
	}
	catch (const std::exception& e) {
		return sq->throwerror(v, e.what());
	}
}

// Commit a transaction
// Usage: postgres_commit(transaction)
// Returns: true on success
_SQUIRRELDEF(SQ_postgres_commit)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER)
	{
		return sq->throwerror(v, "Error in 'postgres_commit': Expected transaction handle");
	}

	void* ptr{};
	sq->getuserpointer(v, 2, &ptr);

	for (auto it = g_transactions.begin(); it != g_transactions.end(); ++it) {
		if (it->get() == ptr) {
			try {
				(*it)->commit();
				g_transactions.erase(it);
				sq->pushbool(v, SQTrue);
				return 1;
			}
			catch (const std::exception& e) {
				return sq->throwerror(v, e.what());
			}
		}
	}

	return sq->throwerror(v, "Error in 'postgres_commit': Invalid transaction handle");
}

// Rollback a transaction
// Usage: postgres_rollback(transaction)
// Returns: true on success
_SQUIRRELDEF(SQ_postgres_rollback)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER)
	{
		return sq->throwerror(v, "Error in 'postgres_rollback': Expected transaction handle");
	}

	void* ptr{};
	sq->getuserpointer(v, 2, &ptr);

	for (auto it = g_transactions.begin(); it != g_transactions.end(); ++it) {
		if (it->get() == ptr) {
			try {
				(*it)->abort();
				g_transactions.erase(it);
				sq->pushbool(v, SQTrue);
				return 1;
			}
			catch (const std::exception& e) {
				return sq->throwerror(v, e.what());
			}
		}
	}

	return sq->throwerror(v, "Error in 'postgres_rollback': Invalid transaction handle");
}

// Execute query within a transaction
// Usage: postgres_query_txn(transaction, query_string)
// Returns: table with {rows: [...], count: n, affected_rows: n}
// Throws: error on failure
_SQUIRRELDEF(SQ_postgres_query_txn)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER || sq->gettype(v, 3) != OT_STRING)
	{
		return sq->throwerror(v, "Error in 'postgres_query_txn': Expected (transaction, query_string)");
	}

	void* ptr{};
	const char* query;

	sq->getuserpointer(v, 2, &ptr);
	sq->getstring(v, 3, &query);

	auto txn = static_cast<pqxx::work*>(ptr);

	try {
		auto result = txn->exec(query);

		// For SELECT queries, return the result as a table directly
		if (is_select_query(query)) {
			push_result_as_table(v, result);
			return 1;
		}
		else {
			// For non-SELECT queries, return table with affected_rows
			sq->newtable(v);
			sq->pushstring(v, "affected_rows", -1);
			sq->pushinteger(v, (SQInteger)result.affected_rows());
			sq->rawset(v, -3);

			sq->pushstring(v, "count", -1);
			sq->pushinteger(v, 0);
			sq->rawset(v, -3);

			sq->pushstring(v, "rows", -1);
			sq->newarray(v, 0);
			sq->rawset(v, -3);

			return 1;
		}
	}
	catch (const std::exception& e) {
		return sq->throwerror(v, e.what());
	}
}

// Execute a PostgreSQL query
// Usage: postgres_query(connection, query_string)
// Returns: table with {rows: [...], count: n, affected_rows: n}
// Throws: error on failure
_SQUIRRELDEF(SQ_postgres_query)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER || sq->gettype(v, 3) != OT_STRING)
	{
		return sq->throwerror(v, "Error in 'postgres_query': Expected (connection, query_string)");
	}

	void* ptr{};
	const char* query;

	sq->getuserpointer(v, 2, &ptr);
	sq->getstring(v, 3, &query);

	auto conn = static_cast<pqxx::connection*>(ptr);

	try {
		pqxx::work txn(*conn);
		auto result = txn.exec(query);
		txn.commit();

		// For SELECT queries, return the result as a table directly
		if (is_select_query(query)) {
			push_result_as_table(v, result);
			return 1;
		}
		else {
			// For non-SELECT queries, return table with affected_rows
			sq->newtable(v);
			sq->pushstring(v, "affected_rows", -1);
			sq->pushinteger(v, (SQInteger)result.affected_rows());
			sq->rawset(v, -3);

			sq->pushstring(v, "count", -1);
			sq->pushinteger(v, 0);
			sq->rawset(v, -3);

			sq->pushstring(v, "rows", -1);
			sq->newarray(v, 0);
			sq->rawset(v, -3);

			return 1;
		}
	}
	catch (const std::exception& e) {
		return sq->throwerror(v, e.what());
	}
}

// Execute a non-transactional query (for DDL)
// Usage: postgres_exec(connection, query_string)
// Returns: table with affected_rows
// Throws: error on failure
_SQUIRRELDEF(SQ_postgres_exec)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER || sq->gettype(v, 3) != OT_STRING)
	{
		return sq->throwerror(v, "Error in 'postgres_exec': Expected (connection, query_string)");
	}

	void* ptr{};
	const char* query;

	sq->getuserpointer(v, 2, &ptr);
	sq->getstring(v, 3, &query);

	auto conn = static_cast<pqxx::connection*>(ptr);

	try {
		pqxx::nontransaction ntxn(*conn);
		auto result = ntxn.exec(query);

		// Return table with affected_rows
		sq->newtable(v);
		sq->pushstring(v, "affected_rows", -1);
		sq->pushinteger(v, (SQInteger)result.affected_rows());
		sq->rawset(v, -3);

		sq->pushstring(v, "count", -1);
		sq->pushinteger(v, 0);
		sq->rawset(v, -3);

		sq->pushstring(v, "rows", -1);
		sq->newarray(v, 0);
		sq->rawset(v, -3);

		return 1;
	}
	catch (const std::exception& e) {
		return sq->throwerror(v, e.what());
	}
}

// Execute a prepared statement (creates and executes in one call)
// Usage: postgres_prepared(connection, query_string, [param1, param2, ...])
// Returns: table with {rows: [...], count: n, affected_rows: n}
// Throws: error on failure
_SQUIRRELDEF(SQ_postgres_prepared)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER || sq->gettype(v, 3) != OT_STRING)
	{
		return sq->throwerror(v, "Error in 'postgres_prepared': Expected at least (connection, query_string)");
	}

	void* ptr{};
	const char* query;

	sq->getuserpointer(v, 2, &ptr);
	sq->getstring(v, 3, &query);

	// Use a simple fixed statement name
	const char* stmt_name = "sq_stmt";

	auto conn = static_cast<pqxx::connection*>(ptr);

	try {
		pqxx::work txn(*conn);

		// Get the number of parameters (arguments beyond connection and query)
		SQInteger top = sq->gettop(v);
		SQInteger param_count = top - 3;

		// Prepare the statement (will replace if it already exists)
		conn->prepare(stmt_name, query);

		// Build parameter list
		std::vector<std::string> params;
		for (SQInteger i = 0; i < param_count; ++i)
		{
			SQInteger idx = 4 + i;
			SQObjectType param_type = sq->gettype(v, idx);

			if (param_type == OT_NULL)
			{
				params.push_back("");
			}
			else if (param_type == OT_INTEGER)
			{
				SQInteger val;
				sq->getinteger(v, idx, &val);
				params.push_back(std::to_string(val));
			}
			else if (param_type == OT_FLOAT)
			{
				SQFloat val;
				sq->getfloat(v, idx, &val);
				params.push_back(std::to_string(val));
			}
			else if (param_type == OT_BOOL)
			{
				SQBool val;
				sq->getbool(v, idx, &val);
				params.push_back(val ? "true" : "false");
			}
			else if (param_type == OT_STRING)
			{
				const char* val;
				sq->getstring(v, idx, &val);
				params.push_back(val);
			}
			else
			{
				conn->unprepare(stmt_name);
				return sq->throwerror(v, "Error in 'postgres_prepared': Invalid parameter type");
			}
		}

		// Execute the prepared statement
		pqxx::result result;
		switch (params.size())
		{
		case 0: result = txn.exec_prepared(stmt_name); break;
		case 1: result = txn.exec_prepared(stmt_name, params[0]); break;
		case 2: result = txn.exec_prepared(stmt_name, params[0], params[1]); break;
		case 3: result = txn.exec_prepared(stmt_name, params[0], params[1], params[2]); break;
		case 4: result = txn.exec_prepared(stmt_name, params[0], params[1], params[2], params[3]); break;
		case 5: result = txn.exec_prepared(stmt_name, params[0], params[1], params[2], params[3], params[4]); break;
		case 6: result = txn.exec_prepared(stmt_name, params[0], params[1], params[2], params[3], params[4], params[5]); break;
		case 7: result = txn.exec_prepared(stmt_name, params[0], params[1], params[2], params[3], params[4], params[5], params[6]); break;
		case 8: result = txn.exec_prepared(stmt_name, params[0], params[1], params[2], params[3], params[4], params[5], params[6], params[7]); break;
		case 9: result = txn.exec_prepared(stmt_name, params[0], params[1], params[2], params[3], params[4], params[5], params[6], params[7], params[8]); break;
		case 10: result = txn.exec_prepared(stmt_name, params[0], params[1], params[2], params[3], params[4], params[5], params[6], params[7], params[8], params[9]); break;
		default:
			conn->unprepare(stmt_name);
			return sq->throwerror(v, "Error in 'postgres_prepared': Too many parameters (max 10)");
		}

		// Unprepare the statement
		conn->unprepare(stmt_name);

		txn.commit();

		// Check if this looks like a SELECT query
		if (result.columns() > 0 && result.size() > 0) {
			push_result_as_table(v, result);
			return 1;
		}
		else {
			// For non-SELECT queries, return table with affected_rows
			sq->newtable(v);
			sq->pushstring(v, "affected_rows", -1);
			sq->pushinteger(v, (SQInteger)result.affected_rows());
			sq->rawset(v, -3);

			sq->pushstring(v, "count", -1);
			sq->pushinteger(v, 0);
			sq->rawset(v, -3);

			sq->pushstring(v, "rows", -1);
			sq->newarray(v, 0);
			sq->rawset(v, -3);

			return 1;
		}
	}
	catch (const std::exception& e) {
		// Unprepare statement on error
		try {
			conn->unprepare(stmt_name);
		}
		catch (...) {
			// Ignore unprepare errors
		}
		return sq->throwerror(v, e.what());
	}
}

// Execute a prepared statement within a transaction
// Usage: postgres_prepared_txn(transaction, query_string, [param1, param2, ...])
// Returns: table with {rows: [...], count: n, affected_rows: n}
// Throws: error on failure
_SQUIRRELDEF(SQ_postgres_prepared_txn)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER || sq->gettype(v, 3) != OT_STRING)
	{
		return sq->throwerror(v, "Error in 'postgres_prepared_txn': Expected at least (transaction, query_string)");
	}

	void* ptr{};
	const char* query;

	sq->getuserpointer(v, 2, &ptr);
	sq->getstring(v, 3, &query);

	auto txn = static_cast<pqxx::work*>(ptr);

	// Use a simple fixed statement name
	const char* stmt_name = "sq_stmt_txn";

	try {
		// Get the number of parameters (arguments beyond transaction and query)
		SQInteger top = sq->gettop(v);
		SQInteger param_count = top - 3;

		// Prepare the statement (will replace if it already exists)
		txn->conn().prepare(stmt_name, query);

		// Build parameter list
		std::vector<std::string> params;
		for (SQInteger i = 0; i < param_count; ++i)
		{
			SQInteger idx = 4 + i;
			SQObjectType param_type = sq->gettype(v, idx);

			if (param_type == OT_NULL)
			{
				params.push_back("");
			}
			else if (param_type == OT_INTEGER)
			{
				SQInteger val;
				sq->getinteger(v, idx, &val);
				params.push_back(std::to_string(val));
			}
			else if (param_type == OT_FLOAT)
			{
				SQFloat val;
				sq->getfloat(v, idx, &val);
				params.push_back(std::to_string(val));
			}
			else if (param_type == OT_BOOL)
			{
				SQBool val;
				sq->getbool(v, idx, &val);
				params.push_back(val ? "true" : "false");
			}
			else if (param_type == OT_STRING)
			{
				const char* val;
				sq->getstring(v, idx, &val);
				params.push_back(val);
			}
			else
			{
				txn->conn().unprepare(stmt_name);
				return sq->throwerror(v, "Error in 'postgres_prepared_txn': Invalid parameter type");
			}
		}

		// Execute the prepared statement
		pqxx::result result;
		switch (params.size())
		{
		case 0: result = txn->exec_prepared(stmt_name); break;
		case 1: result = txn->exec_prepared(stmt_name, params[0]); break;
		case 2: result = txn->exec_prepared(stmt_name, params[0], params[1]); break;
		case 3: result = txn->exec_prepared(stmt_name, params[0], params[1], params[2]); break;
		case 4: result = txn->exec_prepared(stmt_name, params[0], params[1], params[2], params[3]); break;
		case 5: result = txn->exec_prepared(stmt_name, params[0], params[1], params[2], params[3], params[4]); break;
		case 6: result = txn->exec_prepared(stmt_name, params[0], params[1], params[2], params[3], params[4], params[5]); break;
		case 7: result = txn->exec_prepared(stmt_name, params[0], params[1], params[2], params[3], params[4], params[5], params[6]); break;
		case 8: result = txn->exec_prepared(stmt_name, params[0], params[1], params[2], params[3], params[4], params[5], params[6], params[7]); break;
		case 9: result = txn->exec_prepared(stmt_name, params[0], params[1], params[2], params[3], params[4], params[5], params[6], params[7], params[8]); break;
		case 10: result = txn->exec_prepared(stmt_name, params[0], params[1], params[2], params[3], params[4], params[5], params[6], params[7], params[8], params[9]); break;
		default:
			txn->conn().unprepare(stmt_name);
			return sq->throwerror(v, "Error in 'postgres_prepared_txn': Too many parameters (max 10)");
		}

		// Unprepare the statement
		txn->conn().unprepare(stmt_name);

		// Check if this looks like a SELECT query
		if (result.columns() > 0 && result.size() > 0) {
			push_result_as_table(v, result);
			return 1;
		}
		else {
			// For non-SELECT queries, return table with affected_rows
			sq->newtable(v);
			sq->pushstring(v, "affected_rows", -1);
			sq->pushinteger(v, (SQInteger)result.affected_rows());
			sq->rawset(v, -3);

			sq->pushstring(v, "count", -1);
			sq->pushinteger(v, 0);
			sq->rawset(v, -3);

			sq->pushstring(v, "rows", -1);
			sq->newarray(v, 0);
			sq->rawset(v, -3);

			return 1;
		}
	}
	catch (const std::exception& e) {
		// Unprepare statement on error
		try {
			txn->conn().unprepare(stmt_name);
		}
		catch (...) {
			// Ignore unprepare errors
		}
		return sq->throwerror(v, e.what());
	}
}

// Escape string for use in queries
// Usage: postgres_escape_string(connection, string)
// Returns: escaped string
_SQUIRRELDEF(SQ_postgres_escape_string)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER || sq->gettype(v, 3) != OT_STRING)
	{
		return sq->throwerror(v, "Error in 'postgres_escape_string': Expected (connection, string)");
	}

	void* ptr{};
	const char* text;

	sq->getuserpointer(v, 2, &ptr);
	sq->getstring(v, 3, &text);

	auto conn = static_cast<pqxx::connection*>(ptr);

	try {
		std::string escaped = conn->esc(text);
		sq->pushstring(v, escaped.c_str(), escaped.length());
		return 1;
	}
	catch (const std::exception& e)
	{
		return sq->throwerror(v, e.what());
	}
}

// Quote string literal for use in queries (includes quotes)
// Usage: postgres_quote(connection, string)
// Returns: quoted string
_SQUIRRELDEF(SQ_postgres_quote)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER || sq->gettype(v, 3) != OT_STRING)
	{
		return sq->throwerror(v, "Error in 'postgres_quote': Expected (connection, string)");
	}

	void* ptr{};
	const char* text;

	sq->getuserpointer(v, 2, &ptr);
	sq->getstring(v, 3, &text);

	auto conn = static_cast<pqxx::connection*>(ptr);

	try {
		std::string quoted = conn->quote(text);
		sq->pushstring(v, quoted.c_str(), quoted.length());
		return 1;
	}
	catch (const std::exception& e)
	{
		return sq->throwerror(v, e.what());
	}
}

// Quote identifier (table/column name) for use in queries
// Usage: postgres_quote_name(connection, identifier)
// Returns: quoted identifier
_SQUIRRELDEF(SQ_postgres_quote_name)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER || sq->gettype(v, 3) != OT_STRING)
	{
		return sq->throwerror(v, "Error in 'postgres_quote_name': Expected (connection, identifier)");
	}

	void* ptr{};
	const char* text;

	sq->getuserpointer(v, 2, &ptr);
	sq->getstring(v, 3, &text);

	auto conn = static_cast<pqxx::connection*>(ptr);

	try {
		std::string quoted = conn->quote_name(text);
		sq->pushstring(v, quoted.c_str(), quoted.length());
		return 1;
	}
	catch (const std::exception& e)
	{
		return sq->throwerror(v, e.what());
	}
}

// Get database version string
// Usage: postgres_version(connection)
// Returns: version string
_SQUIRRELDEF(SQ_postgres_version)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER)
	{
		return sq->throwerror(v, "Error in 'postgres_version': Expected connection handle");
	}

	void* ptr{};
	sq->getuserpointer(v, 2, &ptr);

	auto conn = static_cast<pqxx::connection*>(ptr);

	try {
		int version = conn->server_version();
		char version_str[64];
		snprintf(version_str, sizeof(version_str), "%d.%d.%d",
			version / 10000,
			(version / 100) % 100,
			version % 100);
		sq->pushstring(v, version_str, -1);
		return 1;
	}
	catch (const std::exception& e)
	{
		return sq->throwerror(v, e.what());
	}
}

// Get database name
// Usage: postgres_dbname(connection)
// Returns: database name
_SQUIRRELDEF(SQ_postgres_dbname)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER)
	{
		return sq->throwerror(v, "Error in 'postgres_dbname': Expected connection handle");
	}

	void* ptr{};
	sq->getuserpointer(v, 2, &ptr);

	auto conn = static_cast<pqxx::connection*>(ptr);

	try {
		std::string dbname = conn->dbname();
		sq->pushstring(v, dbname.c_str(), dbname.length());
		return 1;
	}
	catch (const std::exception& e)
	{
		return sq->throwerror(v, e.what());
	}
}

// Get username
// Usage: postgres_username(connection)
// Returns: username
_SQUIRRELDEF(SQ_postgres_username)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER)
	{
		return sq->throwerror(v, "Error in 'postgres_username': Expected connection handle");
	}

	void* ptr{};
	sq->getuserpointer(v, 2, &ptr);

	auto conn = static_cast<pqxx::connection*>(ptr);

	try {
		std::string username = conn->username();
		sq->pushstring(v, username.c_str(), username.length());
		return 1;
	}
	catch (const std::exception& e)
	{
		return sq->throwerror(v, e.what());
	}
}

// Get hostname
// Usage: postgres_hostname(connection)
// Returns: hostname
_SQUIRRELDEF(SQ_postgres_hostname)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER)
	{
		return sq->throwerror(v, "Error in 'postgres_hostname': Expected connection handle");
	}

	void* ptr{};
	sq->getuserpointer(v, 2, &ptr);

	auto conn = static_cast<pqxx::connection*>(ptr);

	try {
		std::string hostname = conn->hostname();
		sq->pushstring(v, hostname.c_str(), hostname.length());
		return 1;
	}
	catch (const std::exception& e)
	{
		return sq->throwerror(v, e.what());
	}
}

// Get port
// Usage: postgres_port(connection)
// Returns: port number as string
_SQUIRRELDEF(SQ_postgres_port)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER)
	{
		return sq->throwerror(v, "Error in 'postgres_port': Expected connection handle");
	}

	void* ptr{};
	sq->getuserpointer(v, 2, &ptr);

	auto conn = static_cast<pqxx::connection*>(ptr);

	try {
		std::string port = conn->port();
		sq->pushstring(v, port.c_str(), port.length());
		return 1;
	}
	catch (const std::exception& e)
	{
		return sq->throwerror(v, e.what());
	}
}

// Parse JSON string and return as Squirrel table/array
// Usage: postgres_json_decode(json_string)
// Returns: Squirrel value (table, array, string, number, bool, or null)
// Throws: error on invalid JSON
_SQUIRRELDEF(SQ_postgres_json_decode)
{
	if (sq->gettype(v, 2) != OT_STRING)
	{
		return sq->throwerror(v, "Error in 'postgres_json_decode': Expected JSON string");
	}

	const char* json_str;
	sq->getstring(v, 2, &json_str);

	try {
		JSONParser parser(json_str, v, sq);
		if (!parser.parseValue()) {
			return sq->throwerror(v, "Invalid JSON");
		}
		return 1;
	}
	catch (const std::exception& e) {
		return sq->throwerror(v, e.what());
	}
}

// Convert Squirrel value to JSON string
// Usage: postgres_json_encode(value)
// Returns: JSON string
// Throws: error on failure
_SQUIRRELDEF(SQ_postgres_json_encode)
{
	if (sq->gettop(v) < 2) {
		return sq->throwerror(v, "Error in 'postgres_json_encode': Expected a value to encode");
	}

	std::string result;

	// Recursive JSON encoder
	std::function<bool(int)> encodeValue = [&](int idx) -> bool {
		SQObjectType type = sq->gettype(v, idx);

		switch (type) {
		case OT_NULL:
			result += "null";
			return true;

		case OT_INTEGER: {
			SQInteger val;
			sq->getinteger(v, idx, &val);
			result += std::to_string(val);
			return true;
		}

		case OT_FLOAT: {
			SQFloat val;
			sq->getfloat(v, idx, &val);
			result += std::to_string(val);
			return true;
		}

		case OT_BOOL: {
			SQBool val;
			sq->getbool(v, idx, &val);
			result += val ? "true" : "false";
			return true;
		}

		case OT_STRING: {
			const char* val;
			sq->getstring(v, idx, &val);
			result += '"';

			// Escape special characters
			for (const char* p = val; *p; p++) {
				switch (*p) {
				case '"': result += "\\\""; break;
				case '\\': result += "\\\\"; break;
				case '\b': result += "\\b"; break;
				case '\f': result += "\\f"; break;
				case '\n': result += "\\n"; break;
				case '\r': result += "\\r"; break;
				case '\t': result += "\\t"; break;
				default:
					if (*p < 32) {
						char buf[8];
						snprintf(buf, sizeof(buf), "\\u%04x", (unsigned char)*p);
						result += buf;
					}
					else {
						result += *p;
					}
					break;
				}
			}
			result += '"';
			return true;
		}

		case OT_TABLE: {
			result += '{';
			bool first = true;

			// Push the table for iteration
			sq->push(v, idx);

			// Push null iterator
			sq->pushnull(v);

			while (SQ_SUCCEEDED(sq->next(v, -2))) {
				if (!first) result += ',';
				first = false;

				// Key (must be a string for JSON)
				if (sq->gettype(v, -2) == OT_STRING) {
					const char* key;
					sq->getstring(v, -2, &key);

					// Check if key needs escaping
					result += '"';
					for (const char* p = key; *p; p++) {
						switch (*p) {
						case '"': result += "\\\""; break;
						case '\\': result += "\\\\"; break;
						case '\b': result += "\\b"; break;
						case '\f': result += "\\f"; break;
						case '\n': result += "\\n"; break;
						case '\r': result += "\\r"; break;
						case '\t': result += "\\t"; break;
						default:
							if (*p < 32) {
								char buf[8];
								snprintf(buf, sizeof(buf), "\\u%04x", (unsigned char)*p);
								result += buf;
							}
							else {
								result += *p;
							}
							break;
						}
					}
					result += "\":";

					// Value
					if (!encodeValue(-1)) {
						sq->pop(v, 4); // pop value, key, iterator, and table copy
						return false;
					}
				}
				else {
					// Skip non-string keys (JSON only supports string keys)
					sq->pop(v, 2); // pop key and value
					continue;
				}

				sq->pop(v, 2); // pop key and value
			}

			sq->pop(v, 2); // pop iterator and table copy
			result += '}';
			return true;
		}

		case OT_ARRAY: {
			result += '[';

			// Push the array to iterate
			sq->push(v, idx);

			// Push null iterator
			sq->pushnull(v);

			bool first = true;
			while (SQ_SUCCEEDED(sq->next(v, -2))) {
				if (!first) result += ',';
				first = false;

				// Get the value (skip the integer key)
				if (!encodeValue(-1)) {
					sq->pop(v, 4); // pop value, key, iterator, and array copy
					return false;
				}

				sq->pop(v, 2); // pop value and key
			}

			sq->pop(v, 2); // pop iterator and array copy
			result += ']';
			return true;
		}

		default:
			return false;
		}
		};

	try {
		if (!encodeValue(2)) {
			return sq->throwerror(v, "Cannot encode value to JSON");
		}

		sq->pushstring(v, result.c_str(), result.length());
		return 1;
	}
	catch (const std::exception& e) {
		return sq->throwerror(v, e.what());
	}
}

//--------------------------------------------------

SQInteger RegisterSquirrelFunc(HSQUIRRELVM v, SQFUNCTION f, const SQChar* fname, unsigned char ucParams, const SQChar* szParams)
{
	char szNewParams[32];

	sq->pushroottable(v);
	sq->pushstring(v, fname, -1);
	sq->newclosure(v, f, 0);

	if (ucParams > 0)
	{
		ucParams++;

#ifdef _WIN32
		sprintf_s(szNewParams, 32, "t%s", szParams);
#else
		snprintf(szNewParams, 32, "t%s", szParams);
#endif

		sq->setparamscheck(v, ucParams, szNewParams);
	}

	sq->setnativeclosurename(v, -1, fname);
	sq->newslot(v, -3, SQFalse);
	sq->pop(v, 1);

	return 0;
}

void RegisterFuncs(HSQUIRRELVM v)
{
	// Connection functions
	RegisterSquirrelFunc(v, SQ_postgres_connect, "postgres_connect", 0, 0);
	RegisterSquirrelFunc(v, SQ_postgres_close, "postgres_close", 0, 0);
	RegisterSquirrelFunc(v, SQ_postgres_is_open, "postgres_is_open", 0, 0);

	// Transaction management functions
	RegisterSquirrelFunc(v, SQ_postgres_begin, "postgres_begin", 0, 0);
	RegisterSquirrelFunc(v, SQ_postgres_commit, "postgres_commit", 0, 0);
	RegisterSquirrelFunc(v, SQ_postgres_rollback, "postgres_rollback", 0, 0);
	RegisterSquirrelFunc(v, SQ_postgres_query_txn, "postgres_query_txn", 0, 0);
	RegisterSquirrelFunc(v, SQ_postgres_prepared_txn, "postgres_prepared_txn", 0, 0);

	// Query execution functions
	RegisterSquirrelFunc(v, SQ_postgres_query, "postgres_query", 0, 0);
	RegisterSquirrelFunc(v, SQ_postgres_exec, "postgres_exec", 0, 0);

	// Prepared statement function
	RegisterSquirrelFunc(v, SQ_postgres_prepared, "postgres_prepared", 0, 0);

	// String escaping functions
	RegisterSquirrelFunc(v, SQ_postgres_escape_string, "postgres_escape_string", 0, 0);
	RegisterSquirrelFunc(v, SQ_postgres_quote, "postgres_quote", 0, 0);
	RegisterSquirrelFunc(v, SQ_postgres_quote_name, "postgres_quote_name", 0, 0);

	// Connection information functions
	RegisterSquirrelFunc(v, SQ_postgres_version, "postgres_version", 0, 0);
	RegisterSquirrelFunc(v, SQ_postgres_dbname, "postgres_dbname", 0, 0);
	RegisterSquirrelFunc(v, SQ_postgres_username, "postgres_username", 0, 0);
	RegisterSquirrelFunc(v, SQ_postgres_hostname, "postgres_hostname", 0, 0);
	RegisterSquirrelFunc(v, SQ_postgres_port, "postgres_port", 0, 0);

	// JSON functions
	RegisterSquirrelFunc(v, SQ_postgres_json_decode, "postgres_json_decode", 0, 0);
	RegisterSquirrelFunc(v, SQ_postgres_json_encode, "postgres_json_encode", 0, 0);
}