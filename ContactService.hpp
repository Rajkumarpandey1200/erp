#pragma once

#include <string>
#include <vector>
#include <tuple>
#include <database_manager_sqlite.hpp>
#include <grpcpp/grpcpp.h>
#include <contact.grpc.pb.h>
#include <CallDataBase.hpp>
#include <spdlog/spdlog.h>
#include <Logger.hpp>

namespace ContactServiceImpl {
    class AddContact : public CallDataBase {
    public:
        AddContact(erp::ContactService::AsyncService* service, grpc::ServerCompletionQueue* cq, const spdlog::level::level_enum& loglevel)
            : service_(service), cq_(cq), responder_(&ctx_), status_(CallStatus::CREATE), loglevel(loglevel) {
            logger = std::make_shared<spdlog::logger>("AddContact", logger::sink);
            logger->set_level(loglevel);
            db_manager = std::make_unique<DatabaseManagerSqlite>();
            Proceed();
        }

        void Proceed();
        void CleanUp();

    private:
        erp::ContactService::AsyncService* service_;
        grpc::ServerCompletionQueue* cq_;
        grpc::ServerContext ctx_;
        erp::ContactInfo request_;
        erp::InsertionCompleteResponse response_;
        grpc::ServerAsyncResponseWriter<erp::InsertionCompleteResponse> responder_;
        enum class CallStatus { CREATE, PROCESS, FINISH };
        CallStatus status_;
        std::unique_ptr<DatabaseManager> db_manager;
        std::shared_ptr<spdlog::logger> logger;
        spdlog::level::level_enum loglevel;
    };

    
    class ContactGetDetails : public CallDataBase {
    public:
        ContactGetDetails(erp::ContactService::AsyncService* service, grpc::ServerCompletionQueue* cq, const spdlog::level::level_enum& loglevel)
        : service_(service), cq_(cq), responder_(&ctx_), state_(CallStatus::CREATE), loglevel(loglevel) {
            logger = std::make_shared<spdlog::logger>("ContactGetDetails", logger::sink);
            logger->set_level(loglevel);            
            db_manager = std::make_unique<DatabaseManagerSqlite>();
            Proceed();
            
        }
        void Proceed();
        void CleanUp();
                
    private:
        erp::ContactService::AsyncService* service_;
        grpc::ServerCompletionQueue* cq_;
        grpc::ServerContext ctx_;
        erp::GetContactRecords request_;
        erp::ContactInfo reply_;
        grpc::ServerAsyncResponseWriter<erp::ViewContactResponse> responder_;
        enum CallStatus { CREATE, PROCESS, FINISH };
        CallStatus state_;
        std::unique_ptr<DatabaseManager> db_manager;
        std::shared_ptr<spdlog::logger> logger;
        spdlog::level::level_enum loglevel;                      
    };

    class RemoveContact : public CallDataBase {
    public:
        RemoveContact(erp::ContactService::AsyncService* service, grpc::ServerCompletionQueue* cq, const spdlog::level::level_enum& loglevel) :
            service_(service), cq_(cq), responder_(&ctx_), state_(CallStatus::CREATE), loglevel(loglevel) {
            logger = std::make_shared<spdlog::logger>("RemoveContact", logger::sink);
            logger->set_level(loglevel);
            db_manager = std::make_unique<DatabaseManagerSqlite>();
            Proceed();
        }
        void Proceed();
        void CleanUp();
    private:
        erp::ContactService::AsyncService* service_;
        grpc::ServerCompletionQueue* cq_;
        grpc::ServerContext ctx_;
        erp::ContactIdRequest request_;
        grpc::ServerAsyncResponseWriter<erp::RemovalCompleteResponse> responder_;
        enum CallStatus { CREATE, PROCESS, FINISH };
        CallStatus state_;
        std::unique_ptr<DatabaseManager> db_manager;
        std::shared_ptr<spdlog::logger> logger;
        spdlog::level::level_enum loglevel;
    };  

    class GetContactRecord : public CallDataBase {
    public:
        GetContactRecord(erp::ContactService::AsyncService* service, grpc::ServerCompletionQueue* cq, const spdlog::level::level_enum& loglevel)
            : service_(service), cq_(cq), responder_(&ctx_), state_(CallStatus::CREATE), loglevel(loglevel) {
            logger = std::make_shared<spdlog::logger>("GetContactRecord", logger::sink);
            logger->set_level(loglevel);
            db_manager = std::make_unique<DatabaseManagerSqlite>();
            Proceed();
        }
        
        void Proceed();
        void CleanUp();
    private:
        erp::ContactService::AsyncService* service_;
        grpc::ServerCompletionQueue* cq_;
        grpc::ServerContext ctx_;
        erp::ContactIdRequest request_;
        grpc::ServerAsyncResponseWriter<erp::ContactInfo> responder_;
        enum CallStatus { CREATE, PROCESS, FINISH };
        CallStatus state_;
        std::unique_ptr<DatabaseManager> db_manager;
        std::shared_ptr<spdlog::logger> logger;
        spdlog::level::level_enum loglevel;
    }; 
    
    class UpdateContact : public CallDataBase {
    public:
        UpdateContact(erp::ContactService::AsyncService* service, grpc::ServerCompletionQueue* cq, const spdlog::level::level_enum& loglevel) :
            service_(service), cq_(cq), responder_(&ctx_), state_(CallStatus::CREATE), loglevel(loglevel) {
            logger = std::make_shared<spdlog::logger>("UpdateContact", logger::sink);
            logger->set_level(loglevel);
            db_manager = std::make_unique<DatabaseManagerSqlite>();
            Proceed();
        }
        void Proceed();
        void CleanUp();
    private:
        erp::ContactService::AsyncService* service_;
        grpc::ServerCompletionQueue* cq_;
        grpc::ServerContext ctx_;
        erp::ContactInfo request_;
        grpc::ServerAsyncResponseWriter<erp::UpdationCompleteResponse> responder_;
        enum CallStatus { CREATE, PROCESS, FINISH };
        CallStatus state_;
        std::unique_ptr<DatabaseManager> db_manager;
        std::shared_ptr<spdlog::logger> logger;
        spdlog::level::level_enum loglevel;
    };     
    
}