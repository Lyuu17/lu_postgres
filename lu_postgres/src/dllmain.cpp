
#include "dllmain.hpp"
#include "funcs.hpp"
#include <sdk/SQModule.h>
#include <sdk/squirrel.h>
#include <cstdio>

SQAPI sq;
HSQUIRRELVM v;

LU_EXPORT SQRESULT SQLoad( HSQUIRRELVM vm, SQAPI api )
{
	sq = api;
	v = vm;
	
	RegisterFuncs( vm );

	return SQ_OK;
}

LU_EXPORT SQRESULT SQUnload( void )
{
	return SQ_OK;
}

LU_EXPORT SQRESULT SQCallback( unsigned int uiCallback, void* pData )
{
	return SQ_OK;
}

LU_EXPORT SQRESULT SQPulse( void )
{
	return SQ_OK;
}
