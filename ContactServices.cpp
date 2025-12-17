#include <emr/patient/ContactServices.hpp>
#include <absl/log/check.h>
#include <absl/strings/str_cat.h>
#include <fmt/core.h>
#include <grpcpp/server_context.h>
#include <fmt/ranges.h>
#include <Utils.hpp>
#include <absl/strings/str_split.h>
#include <absl/strings/str_join.h>

void ContactServicesImpl::AddContacts::Proceed() {
    if (status_ == CallStatus::CREATE) {
        status_ = CallStatus::PROCESS;
        service_->RequestAddContacts(&ctx_, &request_, &responder_, cq_, cq_, this);
    }
    else if (status_ == CallStatus::PROCESS) {
        // Spawn new handler for concurrency
        new AddContacts(service_, cq_, loglevel);

        // Check for duplicate email
        std::string email_to_check = request_.contact_email();
        auto existing_emails = db_manager->getColumns("PATIENT_CONTACT_INFO", { "trusted_email", "contact_email" });

        bool is_duplicate = false;
        for (const auto& record : existing_emails) {
            for (const auto& email : record) {
                if (email == email_to_check) {
                    is_duplicate = true;
                    break;
                }
            }
            if (is_duplicate) break;
        }

        if (is_duplicate) {
            // Handle duplicate email case
            emr::InsertionCompleteResponse response;
            response.set_operation_status(emr::OperationStatus::failure);
            response.set_assigned_id(-1);
            response.set_insertion_status("Duplicate email found. Contact not added.");
            responder_.Finish(response, grpc::Status::OK, this);
            status_ = CallStatus::FINISH;
            return; // Exit early due to duplicate
        }

        // Proceed with insertion process
        int assigned_contact_id = -1;
        std::vector<std::string> columnNames = { "id" };
        auto records = db_manager->getColumns("PATIENT_CONTACT_INFO", columnNames);
        for (size_t i = 0; i < records.size(); ++i) {
            if (!records[i].empty()) {
                int current_id = std::stoi(records[i][0]);
                if (current_id != static_cast<int>(i + 1)) {
                    assigned_contact_id = static_cast<int>(i + 1);
                    break;
                }
            }
        }
        if (assigned_contact_id == -1) {
            assigned_contact_id = static_cast<int>(records.size()) + 1;
        }

        // Insert contact info
        std::string errorMsg;
        bool contactInserted = db_manager->insertRecord("PATIENT_CONTACT_INFO", {
            { "id", std::to_string(assigned_contact_id), SQLType::INTEGER },
            { "address", request_.address(), SQLType::STRING },
            { "address_line_2", request_.address_line_2(), SQLType::STRING },
            { "city", request_.city(), SQLType::STRING },
            { "state", std::to_string(request_.state()), SQLType::INTEGER },
            { "postal_code", request_.postal_code(), SQLType::STRING },
            { "country", std::to_string(request_.country()), SQLType::INTEGER },
            { "county", request_.county(), SQLType::STRING },
            { "trusted_email", request_.trusted_email(), SQLType::STRING },
            { "contact_email", request_.contact_email(), SQLType::STRING },
            { "emergency_contact_name", request_.emergency_contact_name(), SQLType::STRING },
            { "emergency_phone", request_.emergency_phone(), SQLType::STRING },
            { "home_phone", request_.home_phone(), SQLType::STRING },
            { "work_phone", request_.work_phone(), SQLType::STRING },
            { "mother_name", request_.mother_name(), SQLType::STRING }
            }, errorMsg);

        // Insert additional addresses
        bool allAddressesInserted = true;
        for (const auto& addr : request_.additional_addresses()) {
            bool addrInserted = db_manager->insertRecord("ADDITIONAL_ADDRESS", {
                { "contact_id", std::to_string(assigned_contact_id), SQLType::INTEGER },
                { "address_use", std::to_string(addr.address_use()), SQLType::INTEGER },
                { "address_type", std::to_string(addr.address_type()), SQLType::INTEGER },
                { "address_line_1", addr.address_line_1(), SQLType::STRING },
                { "address_line_2", addr.address_line_2(), SQLType::STRING },
                { "city", addr.city(), SQLType::STRING },
                { "state", std::to_string(addr.state()), SQLType::INTEGER },
                { "postal_code", addr.postal_code(), SQLType::STRING },
                { "country", addr.country(), SQLType::STRING },
                { "county_district", addr.county_district(), SQLType::STRING },
                { "start_date", addr.start_date(), SQLType::STRING },
                { "end_date", addr.end_date(), SQLType::STRING }
                }, errorMsg);

            if (!addrInserted) {
                allAddressesInserted = false;
                logger->error("Failed to insert additional address: {}", errorMsg);
            }
        }

        // Send response
        emr::InsertionCompleteResponse response;
        if (contactInserted && allAddressesInserted) {
            response.set_operation_status(emr::OperationStatus::success);
            response.set_assigned_id(assigned_contact_id);
            response.set_insertion_status("Contact and addresses inserted successfully.");
        }
        else {
            response.set_operation_status(emr::OperationStatus::failure);
            response.set_assigned_id(-1);
            response.set_insertion_status("Failed to insert contact or addresses.");
        }

        responder_.Finish(response, grpc::Status::OK, this);
        status_ = CallStatus::FINISH;
    }
    else if (status_ == CallStatus::FINISH) {
        CleanUp();
    }
}

void ContactServicesImpl::AddContacts::CleanUp() {
    logger->flush();
    delete this;
}


void ContactServicesImpl::UpdateContacts::Proceed() {
    if (status_ == CallStatus::CREATE) {
        logger->info("Starting UpdateContacts request");
        status_ = CallStatus::PROCESS;
        service_->RequestUpdateContacts(&ctx_, &request_, &responder_, cq_, cq_, this);
        return;
    }

    else if (status_ == CallStatus::PROCESS) {
        logger->info("Processing UpdateContacts");
        new UpdateContacts(service_, cq_, loglevel);

        emr::UpdationCompleteResponse response;
        std::string errorMsg;

        const std::string email_to_check = request_.contact_email();

        // Check if contact ID exists
        auto existing_contact = db_manager->getRecordByCondition(
            "PATIENT_CONTACT_INFO",
            "id",
            std::to_string(request_.contacts_id())
        );
        if (existing_contact.empty()) {
            logger->error("Contact ID {} not found", request_.contacts_id());
            response.set_operation_status(emr::OperationStatus::failure);
            response.set_updation_status("Contact ID not found. Update aborted.");
            status_ = CallStatus::FINISH;
            responder_.Finish(response, grpc::Status::OK, this);
            return;
        }

        // Check for duplicate email (excluding current contact)
        auto existing_emails = db_manager->getColumns(
            "PATIENT_CONTACT_INFO",
            { "id", "trusted_email", "contact_email" }
        );

        bool is_duplicate = false;
        for (const auto& record : existing_emails) {
            if (record.size() >= 3) {
                int existing_id = std::stoi(record[0]);
                if (existing_id != request_.contacts_id()) {
                    if (record[1] == email_to_check || record[2] == email_to_check) {
                        is_duplicate = true;
                        break;
                    }
                }
            }
        }
        if (is_duplicate) {
            logger->warn("Duplicate email {} found for contact ID {}", email_to_check, request_.contacts_id());
            response.set_operation_status(emr::OperationStatus::failure);
            response.set_updation_status("Duplicate email found. Contact not updated.");
            status_ = CallStatus::FINISH;
            responder_.Finish(response, grpc::Status::OK, this);
            return;
        }

        // Update contact info
        bool contactUpdated = db_manager->updateRecord(
            "PATIENT_CONTACT_INFO",
            "id",
            { std::to_string(request_.contacts_id()), SQLType::INTEGER },
            {
                { "address", request_.address(), SQLType::STRING },
                { "address_line_2", request_.address_line_2(), SQLType::STRING },
                { "city", request_.city(), SQLType::STRING },
                { "state", std::to_string(request_.state()), SQLType::INTEGER },
                { "postal_code", request_.postal_code(), SQLType::STRING },
                { "country", std::to_string(request_.country()), SQLType::INTEGER },
                { "county", request_.county(), SQLType::STRING },
                { "trusted_email", request_.trusted_email(), SQLType::STRING },
                { "contact_email", request_.contact_email(), SQLType::STRING },
                { "emergency_contact_name", request_.emergency_contact_name(), SQLType::STRING },
                { "emergency_phone", request_.emergency_phone(), SQLType::STRING },
                { "home_phone", request_.home_phone(), SQLType::STRING },
                { "work_phone", request_.work_phone(), SQLType::STRING },
                { "mother_name", request_.mother_name(), SQLType::STRING }
            },
            errorMsg
        );
        if (!contactUpdated) {
            logger->error("Failed to update contact info for ID {}", request_.contacts_id());
        }
        else {
            logger->info("Updated contact info for ID {}", request_.contacts_id());
        }

        // Delete old addresses
        bool addressesDeleted = db_manager->deleteRecord("ADDITIONAL_ADDRESS", "contact_id = " + std::to_string(request_.contacts_id()), errorMsg);
        if (!addressesDeleted) {
            logger->error("Failed to delete old addresses for contact ID {}", request_.contacts_id());
        }
        else {
            logger->info("Deleted old addresses for contact ID {}", request_.contacts_id());
        }

        // Insert new addresses
        bool allAddressesInserted = true;
        int address_count = request_.additional_addresses_size();
        for (int i = 0; i < address_count; ++i) {
            const auto& addr = request_.additional_addresses().Get(i);
            bool inserted = db_manager->insertRecord("ADDITIONAL_ADDRESS", {
                { "contact_id", std::to_string(request_.contacts_id()), SQLType::INTEGER },
                { "address_use", std::to_string(addr.address_use()), SQLType::INTEGER },
                { "address_type", std::to_string(addr.address_type()), SQLType::INTEGER },
                { "address_line_1", addr.address_line_1(), SQLType::STRING },
                { "address_line_2", addr.address_line_2(), SQLType::STRING },
                { "city", addr.city(), SQLType::STRING },
                { "state", std::to_string(addr.state()), SQLType::INTEGER },
                { "postal_code", addr.postal_code(), SQLType::STRING },
                { "country", addr.country(), SQLType::STRING },
                { "county_district", addr.county_district(), SQLType::STRING },
                { "start_date", addr.start_date(), SQLType::STRING },
                { "end_date", addr.end_date(), SQLType::STRING }
                }, errorMsg);

            if (!inserted) {
                allAddressesInserted = false;
                logger->error("Failed to insert address {} for contact ID {}", i, request_.contacts_id());
            }
        }

        // Final response
        if (contactUpdated && addressesDeleted && allAddressesInserted) {
            response.set_operation_status(emr::OperationStatus::success);
            response.set_updation_status("Contact and addresses updated successfully.");
        }
        else {
            response.set_operation_status(emr::OperationStatus::failure);
            response.set_updation_status("Failed to update contact or addresses.");
        }

        status_ = CallStatus::FINISH;
        responder_.Finish(response, grpc::Status::OK, this);
        logger->info("Finished processing UpdateContacts for ID {}", request_.contacts_id());
    }
    else if (status_ == CallStatus::FINISH) {
        // Cleanup
        CleanUp();
    }
}
void ContactServicesImpl::UpdateContacts::CleanUp() {
    logger->flush();
    delete this;
}


void ContactServicesImpl::RemoveContacts::Proceed() {
    if (state_ == CallStatus::CREATE) {
        // Start listening for a new RemoveContacts request
        state_ = CallStatus::PROCESS;
        service_->RequestRemoveContacts(&ctx_, &request_, &responder_, cq_, cq_, this);
    }
    else if (state_ == CallStatus::PROCESS) {
        // Spawn a new handler for subsequent requests
        new RemoveContacts(service_, cq_, loglevel);

        // Prepare the response object
        emr::RemovalCompleteResponse response;

        // Attempt to delete the contact record from the database
        std::string errorMsg;
        bool deleteSuccess = false;

        // Verify if the contact ID exists before attempting deletion
        auto existing_contact = db_manager->getRecordByCondition("PATIENT_CONTACT_INFO", "id", std::to_string(request_.id()));
        if (existing_contact.empty()) {
            // Contact ID does not exist
            response.set_operation_status(emr::OperationStatus::failure);
            response.set_removal_status("Contact ID not found. Deletion failed.");
            logger->warn("Attempted to delete non-existent contact ID {}", request_.id());
        }
        else {
            // Proceed to delete the contact
            deleteSuccess = db_manager->deleteRecord("PATIENT_CONTACT_INFO", "id = " + std::to_string(request_.id()), errorMsg);
            if (deleteSuccess) {
                // Also delete associated additional addresses
                bool addressesDeleted = db_manager->deleteRecord("ADDITIONAL_ADDRESS", "contact_id = " + std::to_string(request_.id()), errorMsg);
                if (addressesDeleted) {
                    response.set_operation_status(emr::OperationStatus::success);
                    response.set_removal_status("Contact and associated addresses deleted successfully.");
                }
                else {
                    // Addresses deletion failed, but contact deletion succeeded
                    response.set_operation_status(emr::OperationStatus::failure);
                    response.set_removal_status("Contact deleted, but failed to delete associated addresses.");
                    logger->error("Failed to delete addresses for contact ID {}", request_.id());
                }
            }
            else {
                // Contact deletion failed
                response.set_operation_status(emr::OperationStatus::failure);
                response.set_removal_status("Failed to delete contact record: " + errorMsg);
                logger->error("Failed to delete contact ID {}: {}", request_.id(), errorMsg);
            }
        }

        // Send the response back to the client
        responder_.Finish(response, grpc::Status::OK, this);
        state_ = CallStatus::FINISH;
    }
    else if (state_ == CallStatus::FINISH) {
        // Cleanup after finishing the call
        CleanUp();
    }
}

void ContactServicesImpl::RemoveContacts::CleanUp() {
    logger->flush();
    delete this;
}

void ContactServicesImpl::FetchContactRecord::Proceed() {
    if (state_ == CallStatus::CREATE) {
        state_ = CallStatus::PROCESS;
        service_->RequestFetchContactRecord(&ctx_, &request_, &responder_, cq_, cq_, this);
    }
    else if (state_ == CallStatus::PROCESS) {
        new FetchContactRecord(service_, cq_, loglevel);

        emr::ContactInfo contact_info;
        std::string contact_id_str = std::to_string(request_.id());

        auto records = db_manager->getRecordByCondition("PATIENT_CONTACT_INFO", "id", contact_id_str);

        if (records.empty()) {
            logger->warn("No contact record found for ID {}", request_.id());
            responder_.Finish(contact_info, grpc::Status::OK, this);
        }
        else {
            const auto& record = records[0];
            contact_info.set_contacts_id(std::stoi(record[0]));
            contact_info.set_address(record[1]);
            contact_info.set_trusted_email(record[2]);
            contact_info.set_contact_email(record[3]);
            contact_info.set_address_line_2(record[4]);
            contact_info.set_city(record[5]);
            contact_info.set_state(static_cast<emr::State>(std::stoi(record[6])));
            contact_info.set_postal_code(record[7]);
            contact_info.set_country(static_cast<emr::Country>(std::stoi(record[8])));
            contact_info.set_county(record[9]);
            contact_info.set_emergency_contact_name(record[10]);
            contact_info.set_emergency_phone(record[11]);
            contact_info.set_home_phone(record[12]);
            contact_info.set_work_phone(record[13]);
            contact_info.set_mother_name(record[14]);

            auto additional_addrs = db_manager->getRecordByCondition("ADDITIONAL_ADDRESS", "contact_id", contact_id_str);
            for (const auto& addr_record : additional_addrs) {
                emr::AdditionalAddress addr;
                addr.set_address_use(static_cast<emr::AdditionalAddress::AddressUse>(std::stoi(addr_record[1])));
                addr.set_address_type(static_cast<emr::AdditionalAddress::AddressType>(std::stoi(addr_record[2])));
                addr.set_address_line_1(addr_record[3]);
                addr.set_address_line_2(addr_record[4]);
                addr.set_city(addr_record[5]);
                addr.set_state(static_cast<emr::State>(std::stoi(addr_record[6])));
                addr.set_postal_code(addr_record[7]);
                addr.set_country(addr_record[8]);  // assuming string country name
                addr.set_county_district(addr_record[9]);
                addr.set_start_date(addr_record[10]);
                addr.set_end_date(addr_record[11]);
                *contact_info.add_additional_addresses() = addr;
            }

            responder_.Finish(contact_info, grpc::Status::OK, this);
        }

        state_ = CallStatus::FINISH;
    }
    else if (state_ == CallStatus::FINISH) {
        CleanUp();
    }
}

void ContactServicesImpl::FetchContactRecord::CleanUp() {
    logger->flush();
    delete this;
}


void ContactServicesImpl::ContactGetDetail::Proceed() {
    if (state_ == CallStatus::CREATE) {
        state_ = CallStatus::PROCESS;
        service_->RequestContactGetDetail(&ctx_, &request_, &responder_, cq_, cq_, this);
    }
    else if (state_ == CallStatus::PROCESS) {
        new ContactGetDetail(service_, cq_, loglevel);

        emr::ViewContactResponse response;

        int start_id = request_.starting_position();
        int end_id = request_.ending_position();

        int total_count = 0;

        for (int id = start_id; id <= end_id; ++id) {
            std::string contact_id_str = std::to_string(id);
            auto records = db_manager->getRecordByCondition("PATIENT_CONTACT_INFO", "id", contact_id_str);

            if (records.empty()) {
                logger->warn("No contact found for ID {}", id);
                continue;
            }

            const auto& record = records[0];
            emr::ContactInfo contact_info;
            contact_info.set_contacts_id(std::stoi(record[0]));
            contact_info.set_address(record[1]);
            contact_info.set_trusted_email(record[2]);
            contact_info.set_contact_email(record[3]);
            contact_info.set_address_line_2(record[4]);
            contact_info.set_city(record[5]);
            contact_info.set_state(static_cast<emr::State>(std::stoi(record[6])));
            contact_info.set_postal_code(record[7]);
            contact_info.set_country(static_cast<emr::Country>(std::stoi(record[8])));
            contact_info.set_county(record[9]);
            contact_info.set_emergency_contact_name(record[10]);
            contact_info.set_emergency_phone(record[11]);
            contact_info.set_home_phone(record[12]);
            contact_info.set_work_phone(record[13]);
            contact_info.set_mother_name(record[14]);

            auto additional_addrs = db_manager->getRecordByCondition("ADDITIONAL_ADDRESS", "contact_id", contact_id_str);
            for (const auto& addr_record : additional_addrs) {
                emr::AdditionalAddress addr;
                addr.set_address_use(static_cast<emr::AdditionalAddress::AddressUse>(std::stoi(addr_record[1])));
                addr.set_address_type(static_cast<emr::AdditionalAddress::AddressType>(std::stoi(addr_record[2])));
                addr.set_address_line_1(addr_record[3]);
                addr.set_address_line_2(addr_record[4]);
                addr.set_city(addr_record[5]);
                addr.set_state(static_cast<emr::State>(std::stoi(addr_record[6])));
                addr.set_postal_code(addr_record[7]);
                addr.set_country(addr_record[8]);
                addr.set_county_district(addr_record[9]);
                addr.set_start_date(addr_record[10]);
                addr.set_end_date(addr_record[11]);
                *contact_info.add_additional_addresses() = addr;
            }

            *response.add_contacts() = contact_info;
            ++total_count;
        }

        response.set_totalnumberofcontacts(total_count);
        responder_.Finish(response, grpc::Status::OK, this);

        state_ = CallStatus::FINISH;
    }
    else if (state_ == CallStatus::FINISH) {
        CleanUp();
    }
}

void ContactServicesImpl::ContactGetDetail::CleanUp() {
    logger->flush();
    delete this;
}