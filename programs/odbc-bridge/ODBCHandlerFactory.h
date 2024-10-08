#pragma once

#include <Interpreters/Context_fwd.h>
#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include "ColumnInfoHandler.h"
#include "IdentifierQuoteHandler.h"
#include "MainHandler.h"
#include "SchemaAllowedHandler.h"
#include <Poco/Logger.h>


namespace DB
{
/** Factory for '/ping', '/', '/columns_info', '/identifier_quote', '/schema_allowed' handlers.
  * Also stores Session pools for ODBC connections
  */
class ODBCBridgeHandlerFactory : public HTTPRequestHandlerFactory, WithContext
{
public:
    ODBCBridgeHandlerFactory(const std::string & name_, ContextPtr context_);

    std::unique_ptr<HTTPRequestHandler> createRequestHandler(const HTTPServerRequest & request) override;

private:
    LoggerPtr log;
    std::string name;
};

}
