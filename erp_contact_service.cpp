#include <filesystem>
#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include <ContactService.hpp>
#include <thread>
#include <atomic>
#include <spdlog/spdlog.h>

class ERPContactServiceTest : public testing::Test {
protected:
    void SetUp() override {
        DeleteFiles();

        std::string server_address("localhost:50051");
        grpc::ServerBuilder builder;

        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder.RegisterService(&contact_service);

        cq = builder.AddCompletionQueue();
        server = builder.BuildAndStart();
        ASSERT_NE(server, nullptr);

        CreateDatabaseTables();

        new ContactServiceImpl::AddContact(&contact_service, cq.get(), spdlog::level::debug);
        new ContactServiceImpl::UpdateContact(&contact_service, cq.get(), spdlog::level::debug);
        new ContactServiceImpl::RemoveContact(&contact_service, cq.get(), spdlog::level::debug);
        new ContactServiceImpl::GetContactRecord(&contact_service, cq.get(), spdlog::level::debug);
        new ContactServiceImpl::ContactGetDetails(&contact_service, cq.get(), spdlog::level::debug);

        shutdown = false;
        poller = std::thread([this] {
            void* tag;
            bool ok;
            while (!shutdown && cq->Next(&tag, &ok)) {
                if (ok) {
                    static_cast<CallDataBase*>(tag)->Proceed();
                    std::this_thread::sleep_for(std::chrono::microseconds(300));
                }
                else {
                    delete static_cast<CallDataBase*>(tag);
                }
            }
            });

        contact_stub = erp::ContactService::NewStub(
            grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()));
    }

    void TearDown() override {
        if (server) {
            server->Shutdown();
            cq->Shutdown();
            shutdown = true;
            poller.join();
        }
    }

    void DeleteFiles() {
        for (const auto& entry : std::filesystem::directory_iterator(std::filesystem::current_path())) {
            if (entry.is_regular_file() && entry.path().filename().string().starts_with("erp.db")) {
                std::filesystem::remove(entry.path());
            }
        }
    }

    void CreateDatabaseTables() {
        std::unique_ptr<DatabaseManager> db_manager = std::make_unique<DatabaseManagerSqlite>();
        db_manager->createTable("CONTACT_INFO", {
            {"id", "INTEGER", "PRIMARY KEY"},
            {"first_name", "TEXT", ""},
            {"middle_name", "TEXT", ""},
            {"last_name", "TEXT", ""},
            {"salutation", "INTEGER", ""},
            {"designation", "TEXT", ""},
            {"organization", "TEXT", ""},
            {"email_ids", "TEXT", ""},
            {"contact_numbers", "TEXT", ""},
            {"gender", "INTEGER", ""},
            {"status", "INTEGER", ""}
            });
    }

    std::unique_ptr<grpc::Server> server;
    std::unique_ptr<grpc::ServerCompletionQueue> cq;
    std::thread poller;
    std::atomic<bool> shutdown{ false };

    erp::ContactService::AsyncService contact_service;
    std::unique_ptr<erp::ContactService::Stub> contact_stub;
};

TEST_F(ERPContactServiceTest, AddFetchUpdateRemoveContactFlow) {
    grpc::ClientContext ctx_add, ctx_get, ctx_update, ctx_remove, ctx_view;

    // Add Contact
    erp::ContactInfo contact;
    contact.set_first_name("Raj");
    contact.set_middle_name("Kumar");
    contact.set_last_name("Pandey");
    contact.set_salutation(erp::MR);
    contact.set_designation("Developer");
    contact.set_organization("ERP Corp");
    contact.set_gender(erp::MALE);
    contact.set_status(erp::ACTIVE);

    auto* email_entry = contact.add_contact_emails();
    email_entry->set_address_type(erp::PRIMARY);
    email_entry->set_email("prajkumar1200@gmail.com");

    auto* phone = contact.add_contact_numbers();
    phone->set_number_type(erp::PRIMARY);
    phone->set_contact_number("1234567890");


    erp::InsertionCompleteResponse add_response;
    grpc::Status status = contact_stub->AddContact(&ctx_add, contact, &add_response);
    ASSERT_TRUE(status.ok());
    int contact_id = add_response.assigned_id();
    ASSERT_GT(contact_id, 0);

    // Fetch Contact
    erp::ContactIdRequest fetch_request;
    fetch_request.set_contact_id(contact_id);
    erp::ContactInfo fetched;
    status = contact_stub->GetContactRecord(&ctx_get, fetch_request, &fetched);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(fetched.first_name(), "Raj");

    // Update Contact
    fetched.set_designation("Senior Developer");
    fetched.mutable_contact_numbers(0)->set_contact_number("9999999999");

    erp::UpdationCompleteResponse update_response;
    status = contact_stub->UpdateContact(&ctx_update, fetched, &update_response);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(update_response.operation_status(), erp::success);

    // Re-verify
    erp::ContactInfo updated;
    grpc::ClientContext ctx_verify;
    status = contact_stub->GetContactRecord(&ctx_verify, fetch_request, &updated);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(updated.designation(), "Senior Developer");
    ASSERT_EQ(updated.contact_numbers(0).contact_number(), "9999999999");

    // Remove Contact
    erp::RemovalCompleteResponse remove_response;
    status = contact_stub->RemoveContact(&ctx_remove, fetch_request, &remove_response);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(remove_response.operation_status(), erp::success);

    // Confirm deletion
    grpc::ClientContext ctx_get2;
    erp::ContactInfo get_after_delete;
    status = contact_stub->GetContactRecord(&ctx_get2, fetch_request, &get_after_delete);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(get_after_delete.contact_emails_size(), 0);
    ASSERT_EQ(get_after_delete.contact_numbers_size(), 0);
    ASSERT_TRUE(get_after_delete.first_name().empty());

}
