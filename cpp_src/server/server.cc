#include "server.h"
#include "serverimpl.h"

namespace reindexer_server {

Server::Server() : impl_(new ServerImpl) {}
Server::~Server() {}
Error Server::InitFromCLI(int argc, char *argv[]) { return impl_->InitFromCLI(argc, argv); }
Error Server::InitFromFile(const char *filePath) { return impl_->InitFromFile(filePath); }
Error Server::InitFromYAML(const string &yaml) { return impl_->InitFromYAML(yaml); }
int Server::Start() { return impl_->Start(); }
void Server::Stop() { return impl_->Stop(); }
void Server::EnableHandleSignals(bool enable) { impl_->EnableHandleSignals(enable); }
DBManager &Server::GetDBManager() { return impl_->GetDBManager(); }
bool Server::IsReady() { return impl_->IsReady(); }
void Server::ReopenLogFiles() { impl_->ReopenLogFiles(); }

}  // namespace reindexer_server
