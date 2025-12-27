
#include "funcs.hpp"
#include <pqxx/pqxx>
#include <sdk/SQModule.h>
#include <sdk/squirrel.h>
#include <cstdio>
#include <exception>
#include <vector>
#include <memory>
#include <cctype>
#include <pqxx/connection.hxx>
#include <pqxx/internal/libpq-forward.hxx>
#include <pqxx/nontransaction.hxx>
#include <pqxx/result.hxx>
#include <pqxx/transaction.hxx>
#include <string>

extern SQAPI sq;
extern HSQUIRRELVM v;

// Global storage for connections, transactions, and results
std::vector<std::unique_ptr<pqxx::connection>> g_connections;
std::vector<std::unique_ptr<pqxx::work>> g_transactions;
std::vector<std::unique_ptr<pqxx::result>> g_results;

// Helper structure to track result iteration state
struct ResultIterator {
	pqxx::result* result;
	size_t current_row;

	ResultIterator(pqxx::result* res) : result(res), current_row(0) {}
};

std::vector<std::unique_ptr<ResultIterator>> g_iterators;

// Helper function to check if query is a SELECT statement
static bool is_select_query(const char* query) {
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

// Helper function to add result rows to Squirrel table
static void push_result_as_table(HSQUIRRELVM v, const pqxx::result& result) {
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

					if (type_oid == 21 || type_oid == 23 || type_oid == 20)
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
// Returns: connection handle or null on error
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
			sq->pushnull(v);
			return sq->throwerror(v, "Failed to open connection");
		}

		sq->pushuserpointer(v, conn.get());
		return 1;
	}
	catch (const std::exception& e) {
		sq->pushnull(v);
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
// Returns: transaction handle or null on error
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
		sq->pushnull(v);
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
// Returns: table (for SELECT) or result handle (for other queries) or null on error
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
			// For non-SELECT queries, store result and return handle
			auto& stored_result = g_results.emplace_back(std::make_unique<pqxx::result>(result));
			auto& iterator = g_iterators.emplace_back(std::make_unique<ResultIterator>(stored_result.get()));

			sq->pushuserpointer(v, iterator.get());
			return 1;
		}
	}
	catch (const std::exception& e) {
		sq->pushnull(v);
		return sq->throwerror(v, e.what());
	}
}

// Execute a PostgreSQL query
// Usage: postgres_query(connection, query_string)
// Returns: table (for SELECT) or result handle (for other queries) or null on error
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
			// For non-SELECT queries, store result and return handle
			auto& stored_result = g_results.emplace_back(std::make_unique<pqxx::result>(result));
			auto& iterator = g_iterators.emplace_back(std::make_unique<ResultIterator>(stored_result.get()));

			sq->pushuserpointer(v, iterator.get());
			return 1;
		}
	}
	catch (const std::exception& e) {
		sq->pushnull(v);
		return sq->throwerror(v, e.what());
	}
}

// Execute a non-transactional query (for DDL)
// Usage: postgres_exec(connection, query_string)
// Returns: result handle or null on error
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

		auto& stored_result = g_results.emplace_back(std::make_unique<pqxx::result>(result));
		auto& iterator = g_iterators.emplace_back(std::make_unique<ResultIterator>(stored_result.get()));

		sq->pushuserpointer(v, iterator.get());
		return 1;
	}
	catch (const std::exception& e) {
		sq->pushnull(v);
		return sq->throwerror(v, e.what());
	}
}

// Prepare a SQL statement
// Usage: postgres_prepare(connection, statement_name, query_string)
// Returns: true on success, null on error
_SQUIRRELDEF(SQ_postgres_prepare)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER ||
		sq->gettype(v, 3) != OT_STRING ||
		sq->gettype(v, 4) != OT_STRING)
	{
		return sq->throwerror(v, "Error in 'postgres_prepare': Expected (connection, statement_name, query_string)");
	}

	void* ptr{};
	const char* stmt_name;
	const char* query;

	sq->getuserpointer(v, 2, &ptr);
	sq->getstring(v, 3, &stmt_name);
	sq->getstring(v, 4, &query);

	auto conn = static_cast<pqxx::connection*>(ptr);

	try {
		conn->prepare(stmt_name, query);
		sq->pushbool(v, SQTrue);
		return 1;
	}
	catch (const std::exception& e) {
		sq->pushnull(v);
		return sq->throwerror(v, e.what());
	}
}

// Execute a prepared statement
// Usage: postgres_execute(connection, statement_name, [param1, param2, ...])
// Returns: table (for SELECT) or result handle (for other queries) or null on error
_SQUIRRELDEF(SQ_postgres_execute)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER || sq->gettype(v, 3) != OT_STRING)
	{
		return sq->throwerror(v, "Error in 'postgres_execute': Expected at least (connection, statement_name)");
	}

	void* ptr{};
	const char* stmt_name;

	sq->getuserpointer(v, 2, &ptr);
	sq->getstring(v, 3, &stmt_name);

	auto conn = static_cast<pqxx::connection*>(ptr);

	try {
		pqxx::work txn(*conn);

		// Get the number of parameters (arguments beyond connection and statement name)
		SQInteger top = sq->gettop(v);
		SQInteger param_count = top - 3;

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
				return sq->throwerror(v, "Error in 'postgres_execute': Invalid parameter type");
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
			return sq->throwerror(v, "Error in 'postgres_execute': Too many parameters (max 10)");
		}

		txn.commit();

		// Check if this looks like a SELECT query by checking if we have columns and rows
		if (result.columns() > 0 && result.size() > 0) {
			push_result_as_table(v, result);
			return 1;
		}
		else {
			// For non-SELECT queries, store result and return handle
			auto& stored_result = g_results.emplace_back(std::make_unique<pqxx::result>(result));
			auto& iterator = g_iterators.emplace_back(std::make_unique<ResultIterator>(stored_result.get()));

			sq->pushuserpointer(v, iterator.get());
			return 1;
		}
	}
	catch (const std::exception& e) {
		sq->pushnull(v);
		return sq->throwerror(v, e.what());
	}
}

// Unprepare a SQL statement
// Usage: postgres_unprepare(connection, statement_name)
// Returns: true on success
_SQUIRRELDEF(SQ_postgres_unprepare)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER || sq->gettype(v, 3) != OT_STRING)
	{
		return sq->throwerror(v, "Error in 'postgres_unprepare': Expected (connection, statement_name)");
	}

	void* ptr{};
	const char* stmt_name;

	sq->getuserpointer(v, 2, &ptr);
	sq->getstring(v, 3, &stmt_name);

	auto conn = static_cast<pqxx::connection*>(ptr);

	try {
		conn->unprepare(stmt_name);
		sq->pushbool(v, SQTrue);
		return 1;
	}
	catch (const std::exception& e) {
		return sq->throwerror(v, e.what());
	}
}

// Get number of affected rows from last query
// Usage: postgres_affected_rows(result)
// Returns: number of affected rows
_SQUIRRELDEF(SQ_postgres_affected_rows)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER)
	{
		return sq->throwerror(v, "Error in 'postgres_affected_rows': Expected result handle");
	}

	void* ptr{};
	sq->getuserpointer(v, 2, &ptr);

	auto iterator = static_cast<ResultIterator*>(ptr);
	sq->pushinteger(v, (SQInteger)iterator->result->affected_rows());
	return 1;
}

// Free result and associated resources
// Usage: postgres_free_result(result)
// Returns: true on success
_SQUIRRELDEF(SQ_postgres_free_result)
{
	if (sq->gettype(v, 2) != OT_USERPOINTER)
	{
		return sq->throwerror(v, "Error in 'postgres_free_result': Expected result handle");
	}

	void* ptr{};
	sq->getuserpointer(v, 2, &ptr);

	// Remove the iterator
	for (auto it = g_iterators.begin(); it != g_iterators.end(); ++it)
	{
		if (it->get() == ptr)
		{
			auto result_ptr = (*it)->result;
			g_iterators.erase(it);

			// Remove the result
			for (auto res_it = g_results.begin(); res_it != g_results.end(); ++res_it)
			{
				if (res_it->get() == result_ptr)
				{
					g_results.erase(res_it);
					break;
				}
			}

			sq->pushbool(v, SQTrue);
			return 1;
		}
	}

	return sq->throwerror(v, "Error in 'postgres_free_result': Invalid result handle");
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

	// Query execution functions
	RegisterSquirrelFunc(v, SQ_postgres_query, "postgres_query", 0, 0);
	RegisterSquirrelFunc(v, SQ_postgres_exec, "postgres_exec", 0, 0);

	// Prepared statement functions
	RegisterSquirrelFunc(v, SQ_postgres_prepare, "postgres_prepare", 0, 0);
	RegisterSquirrelFunc(v, SQ_postgres_execute, "postgres_execute", 0, 0);
	RegisterSquirrelFunc(v, SQ_postgres_unprepare, "postgres_unprepare", 0, 0);

	// Result functions
	RegisterSquirrelFunc(v, SQ_postgres_affected_rows, "postgres_affected_rows", 0, 0);
	RegisterSquirrelFunc(v, SQ_postgres_free_result, "postgres_free_result", 0, 0);

	// String escaping functions
	RegisterSquirrelFunc(v, SQ_postgres_escape_string, "postgres_escape_string", 0, 0);
	RegisterSquirrelFunc(v, SQ_postgres_quote, "postgres_quote", 0, 0);
	RegisterSquirrelFunc(v, SQ_postgres_quote_name, "postgres_quote_name", 0, 0);
}