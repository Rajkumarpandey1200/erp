#include <ContactService.hpp>
#include <absl/log/check.h>
#include <absl/strings/str_cat.h>
#include <fmt/core.h>
#include <grpcpp/server_context.h>
#include <fmt/ranges.h>
#include <Utils.hpp>
#include <absl/strings/str_split.h>

void ContactServiceImpl::AddContact::Proceed() {
    if (status_ == CallStatus::CREATE) {
        status_ = CallStatus::PROCESS;
        service_->RequestAddContact(&ctx_, &request_, &responder_, cq_, cq_, this);
    }
    else if (status_ == CallStatus::PROCESS) {
        // Spawn new handler for next request
        new AddContact(service_, cq_, loglevel);

        logger->debug("Processing AddContact request for: {}", request_.first_name());

        std::string primaryEmail;
        for (const auto& email : request_.contact_emails()) {
            if (email.address_type() == erp::Type::PRIMARY) {
                primaryEmail = email.email();
                break;
            }
        }

        std::vector<std::string> columnNames = { "email_ids" };
        auto records = db_manager->getColumns("CONTACT_INFO", columnNames);
        bool duplicateFound = false;

        for (const auto& record : records) {
            if (!record.empty()) {
                std::vector<std::string> emails = absl::StrSplit(record[0], ',');
                if (!emails.empty() && emails[0] == primaryEmail) {
                    duplicateFound = true;
                    break;
                }
            }
        }

        if (duplicateFound) {
            response_.set_operation_status(erp::OperationStatus::failure);
            response_.set_assigned_id(-1);
            response_.set_insertion_status("UNIQUE CONSTRAINT FAILED: Primary email already exists");
        }
        else {
            int assigned_contact_id = -1;
            auto idRecords = db_manager->getColumns("CONTACT_INFO", { "id" });

            for (size_t i = 0; i < idRecords.size(); ++i) {
                if (!idRecords[i].empty()) {
                    int current_id = std::stoi(idRecords[i][0]);
                    if (current_id != i + 1) {
                        assigned_contact_id = i + 1;
                        break;
                    }
                }
            }
            if (assigned_contact_id == -1) {
                assigned_contact_id = static_cast<int>(idRecords.size()) + 1;
            }

            std::string emailIdsStr;
            for (const auto& email : request_.contact_emails()) {
                if (!emailIdsStr.empty()) absl::StrAppend(&emailIdsStr, ",");
                absl::StrAppend(&emailIdsStr, email.email());
            }

            std::string contactNumbersStr;
            for (const auto& number : request_.contact_numbers()) {
                if (!contactNumbersStr.empty()) absl::StrAppend(&contactNumbersStr, ",");
                absl::StrAppend(&contactNumbersStr, number.contact_number());
            }

            std::string errorMsg;
            bool success = db_manager->insertRecord("CONTACT_INFO", {
                {"id", std::to_string(assigned_contact_id), SQLType::INTEGER},
                {"first_name", request_.first_name(), SQLType::STRING},
                {"middle_name", request_.has_middle_name() ? request_.middle_name() : "", SQLType::STRING},
                {"last_name", request_.last_name(), SQLType::STRING},
                {"designation", request_.designation(), SQLType::STRING},
                {"email_ids", emailIdsStr, SQLType::STRING},
                {"contact_numbers", contactNumbersStr, SQLType::STRING},
                {"organization", request_.organization(), SQLType::STRING},
                {"gender", std::to_string(static_cast<int>(request_.gender())), SQLType::INTEGER},
                {"salutation", std::to_string(static_cast<int>(request_.salutation())), SQLType::INTEGER},
                {"status", std::to_string(static_cast<int>(request_.status())), SQLType::INTEGER}
                }, errorMsg);

            if (success) {
                response_.set_operation_status(erp::OperationStatus::success);
                response_.set_assigned_id(assigned_contact_id);
                response_.set_insertion_status("Contact inserted successfully");
            }
            else {
                response_.set_operation_status(erp::OperationStatus::failure);
                response_.set_assigned_id(-1);
                response_.set_insertion_status(fmt::format("Failed to insert contact: {}", errorMsg));
            }
        }

        status_ = CallStatus::FINISH;
        responder_.Finish(response_, grpc::Status::OK, this);
    }
    else if (status_ == CallStatus::FINISH) {
        CleanUp();
    }
}

void ContactServiceImpl::AddContact::CleanUp() {
    logger->flush();
    delete this;
}






void ContactServiceImpl::ContactGetDetails::Proceed() {
    if (state_ == CallStatus::CREATE) {
        state_ = CallStatus::PROCESS;
        service_->RequestContactGetDetails(&ctx_, &request_, &responder_, cq_, cq_, this);
    } else if (state_ == CallStatus::PROCESS) {
        new ContactGetDetails(service_, cq_, loglevel);
        int start = request_.startingposition() - 1; 
        int end = request_.endingposition() - 1;     
        int totalRecords = db_manager->getTotalRecords("CONTACT_INFO");
        erp::ViewContactResponse response;
        if (totalRecords == 0) {
            response.set_totalnumberofcontacts(0);
            logger->debug("No contact records exist");
        } else {
            if (end > totalRecords) {
                end = totalRecords;
                logger->debug("Requested number of records exceeds the existing number");
            }
            if (start < 0) {
                start = 0;
                logger->debug("Starting position less than 1. Corrected to first position");
            }

            std::vector<std::vector<std::string>> contactData = db_manager->getRecordsInRange("CONTACT_INFO","id", start, end);
            response.set_totalnumberofcontacts(totalRecords);
            for (const auto& contact : contactData) {
                erp::ContactInfo* contactInfo = response.add_contacts();
                contactInfo->set_contact_id(std::stoi(contact[0]));
                contactInfo->set_first_name(contact[1]);
                if (!contact[2].empty()) {
                    contactInfo->set_middle_name(contact[2]);
                }
                contactInfo->set_last_name(contact[3]);
                contactInfo->set_designation(contact[5]);
                contactInfo->set_organization(contact[6]);
                contactInfo->set_gender(static_cast<erp::Gender>(std::stoi(contact[9])));
                contactInfo->set_salutation(static_cast<erp::Salutation>(std::stoi(contact[4])));
                contactInfo->set_status(static_cast<erp::ContactStatus>(std::stoi(contact[10])));
                
                const std::string emailStr = contact[7];
                const std::string contactNumberStr = contact[8];
                
                std::vector<std::string> emails = absl::StrSplit(emailStr, ',');
                if (!emails.empty()) {
                    erp::ContactEmailAddress* primaryEmail = contactInfo->add_contact_emails();
                    primaryEmail->set_email(emails[0]);
                    primaryEmail->set_address_type(erp::Type::PRIMARY);
                
                    for (size_t i = 1; i < emails.size(); ++i) {
                        if (!emails[i].empty()) {
                            erp::ContactEmailAddress* secondaryEmail = contactInfo->add_contact_emails();
                            secondaryEmail->set_email(emails[i]);
                            secondaryEmail->set_address_type(erp::Type::SECONDARY);
                        }
                    }
                }
                std::vector<std::string> contactNumbers = absl::StrSplit(contactNumberStr, ',');
                if (!contactNumbers.empty()) {
                    erp::ContactNumber* primaryNumber = contactInfo->add_contact_numbers();
                    primaryNumber->set_contact_number(contactNumbers[0]);
                    primaryNumber->set_number_type(erp::Type::PRIMARY);
                    for (size_t i = 1; i < contactNumbers.size(); ++i) {
                        if (!contactNumbers[i].empty()) {
                            erp::ContactNumber* secondaryNumber = contactInfo->add_contact_numbers();
                            secondaryNumber->set_contact_number(contactNumbers[i]);
                            secondaryNumber->set_number_type(erp::Type::SECONDARY);
                        }
                    }
                }
            }
        }

        state_ = CallStatus::FINISH;
        responder_.Finish(response, grpc::Status::OK, this);
    } else {
        CHECK(state_ == CallStatus::FINISH);
        CleanUp();
    }
} 

void ContactServiceImpl::ContactGetDetails::CleanUp() {
    logger->flush();
    delete this;
}






void ContactServiceImpl::RemoveContact::Proceed() {
    if (state_ == CallStatus::CREATE) {
        state_ = CallStatus::PROCESS;
        service_->RequestRemoveContact(&ctx_, &request_, &responder_, cq_, cq_, this);
    } else if (state_ == CallStatus::PROCESS) {
        new RemoveContact(service_, cq_, loglevel);
        erp::RemovalCompleteResponse response;
        std::string error_msg;
        std::vector<std::vector<std::string>> record = db_manager->getRecordByCondition(
            "CONTACT_INFO", "id", fmt::format("{}", request_.contact_id())
        );

        if (record.empty()) {
            response.set_operation_status(erp::OperationStatus::failure);
            response.set_removal_status("Contact not found");
            logger->debug("Contact with ID-{} doesn't exist in records", request_.contact_id());
        } else {
            bool success = db_manager->deleteRecord(
                "CONTACT_INFO", fmt::format("id = {}", request_.contact_id()), error_msg
            );

            if (success) {
                response.set_operation_status(erp::OperationStatus::success);
                logger->debug("Contact with ID-{} removed successfully", request_.contact_id());
            } else {
                response.set_operation_status(erp::OperationStatus::failure);
                response.set_removal_status(fmt::format("Error encountered: {}", error_msg));
                logger->debug("Error in removing contact with ID-{}: {}", request_.contact_id(), error_msg);
            }
        }
        state_ = CallStatus::FINISH;
        responder_.Finish(response, grpc::Status::OK, this);
    } else {
        CHECK(state_ == CallStatus::FINISH);
        CleanUp();
    }
}

void ContactServiceImpl::RemoveContact::CleanUp() {
    logger->flush();
    delete this;
}






void ContactServiceImpl::GetContactRecord::Proceed() {
    if (state_ == CallStatus::CREATE) {
        state_ = CallStatus::PROCESS;
        service_->RequestGetContactRecord(&ctx_, &request_, &responder_, cq_, cq_, this);
    } else if (state_ == CallStatus::PROCESS) {
        new GetContactRecord(service_, cq_, loglevel);
        erp::ContactInfo response;
        std::vector<std::vector<std::string>> record = db_manager->getRecordByCondition(
            "CONTACT_INFO", "id", fmt::format("{}", request_.contact_id())
        );

        if (!record.empty()) {
            response.set_contact_id(std::stoi(record[0][0]));
            response.set_first_name(record[0][1]);
            if (!record[0][2].empty()) {
                response.set_middle_name(record[0][2]);
            }
            response.set_last_name(record[0][3]);
            response.set_designation(record[0][5]);
            response.set_organization(record[0][6]);
            response.set_gender(static_cast<erp::Gender>(std::stoi(record[0][9])));
            response.set_salutation(static_cast<erp::Salutation>(std::stoi(record[0][4])));
            response.set_status(static_cast<erp::ContactStatus>(std::stoi(record[0][10])));                        
            const std::string &emailStr = record[0][7];
            const std::string &contactNumberStr = record[0][8];

            std::vector<std::string> emails = absl::StrSplit(emailStr, ',');
            if (!emails.empty()) {
                if (!emails[0].empty()) {
                    erp::ContactEmailAddress* primaryEmail = response.add_contact_emails();
                    primaryEmail->set_email(emails[0]);
                    primaryEmail->set_address_type(erp::Type::PRIMARY);
                }
                for (size_t i = 1; i < emails.size(); ++i) {
                    if (!emails[i].empty()) {
                        erp::ContactEmailAddress* secondaryEmail = response.add_contact_emails();
                        secondaryEmail->set_email(emails[i]);
                        secondaryEmail->set_address_type(erp::Type::SECONDARY);
                    }
                }
            }
            
            std::vector<std::string> contactNumbers = absl::StrSplit(contactNumberStr, ',');
            if (!contactNumbers.empty()) {
                if (!contactNumbers[0].empty()) {
                    erp::ContactNumber* primaryNumber = response.add_contact_numbers();
                    primaryNumber->set_contact_number(contactNumbers[0]);
                    primaryNumber->set_number_type(erp::Type::PRIMARY);
                }
                for (size_t i = 1; i < contactNumbers.size(); ++i) {
                    if (!contactNumbers[i].empty()) {
                        erp::ContactNumber* secondaryNumber = response.add_contact_numbers();
                        secondaryNumber->set_contact_number(contactNumbers[i]);
                        secondaryNumber->set_number_type(erp::Type::SECONDARY);
                    }
                }
            }
        } else {
            response.set_contact_id(0); 
            logger->debug("Contact with id {} can't be found", request_.contact_id());
        }
        state_ = CallStatus::FINISH;
        responder_.Finish(response, grpc::Status::OK, this);
    } else {
        CHECK(state_ == CallStatus::FINISH);
        CleanUp();
    }
}

void ContactServiceImpl::GetContactRecord::CleanUp() {
    logger->flush();
    delete this;
}







void ContactServiceImpl::UpdateContact::Proceed() {
    if (state_ == CallStatus::CREATE) {
        state_ = CallStatus::PROCESS;
        service_->RequestUpdateContact(&ctx_, &request_, &responder_, cq_, cq_, this);
    } else if (state_ == CallStatus::PROCESS) {
        new UpdateContact(service_, cq_, loglevel);
        erp::UpdationCompleteResponse response;
        std::string error_msg;
        std::string condition = fmt::format("id = {}", request_.contact_id());

        std::string newPrimaryEmail;
        for (const auto& email : request_.contact_emails()) {
            if (email.address_type() == erp::Type::PRIMARY) {
                newPrimaryEmail = email.email();
                break;
            }
        }
        bool duplicatePrimaryEmail = false;
        if (!newPrimaryEmail.empty()) {
            std::vector<std::string> columnNames = {"id", "email_ids"};
            auto records = db_manager->getColumns("CONTACT_INFO", columnNames);
            for (const auto& record : records) {
                if (!record.empty() && record[0] != std::to_string(request_.contact_id())) {
                    std::vector<std::string> emails = absl::StrSplit(record[1], ',');
                    if (!emails.empty() && emails[0] == newPrimaryEmail) {
                        duplicatePrimaryEmail = true;
                        break;
                    }
                }
            }
        }

        if (duplicatePrimaryEmail) {
            response.set_operation_status(erp::OperationStatus::failure);
            response.set_updation_status("UNIQUE CONSTRAINT FAILED: Primary email already exists in another contact");
            logger->debug("Primary email conflict for contact ID-{}", request_.contact_id());
        } else {
            std::vector<std::tuple<std::string, std::string, SQLType>> columnValuePairs;
            columnValuePairs.emplace_back("id", std::to_string(request_.contact_id()), SQLType::INTEGER);
            columnValuePairs.emplace_back("first_name", request_.first_name(), SQLType::STRING);
            columnValuePairs.emplace_back("middle_name", request_.has_middle_name() ? request_.middle_name() : "", SQLType::STRING);
            columnValuePairs.emplace_back("last_name", request_.last_name(), SQLType::STRING);
            columnValuePairs.emplace_back("salutation", std::to_string(static_cast<int>(request_.salutation())), SQLType::INTEGER);
            columnValuePairs.emplace_back("designation", request_.designation(), SQLType::STRING);
            columnValuePairs.emplace_back("organization", request_.organization(), SQLType::STRING);        

            std::string emailIdsStr;
            for (const auto& email : request_.contact_emails()) {
                if (email.address_type() == erp::Type::PRIMARY) {
                    emailIdsStr = absl::StrCat(email.email(), emailIdsStr.empty() ? "" : ",", emailIdsStr);
                } else {
                    if (!emailIdsStr.empty()) {
                        absl::StrAppend(&emailIdsStr, ",");
                    }
                    absl::StrAppend(&emailIdsStr, email.email());
                }
            }
            columnValuePairs.emplace_back("email_ids", emailIdsStr, SQLType::STRING);

            std::string contactNumbersStr;
            for (const auto& number : request_.contact_numbers()) {
                if (number.number_type() == erp::Type::PRIMARY) {
                    contactNumbersStr = absl::StrCat(number.contact_number(), contactNumbersStr.empty() ? "" : ",", contactNumbersStr);
                } else {
                    if (!contactNumbersStr.empty()) {
                        absl::StrAppend(&contactNumbersStr, ",");
                    }
                    absl::StrAppend(&contactNumbersStr, number.contact_number());
                }
            }
            columnValuePairs.emplace_back("contact_numbers", contactNumbersStr, SQLType::STRING);

            columnValuePairs.emplace_back("gender", std::to_string(static_cast<int>(request_.gender())), SQLType::INTEGER);
            columnValuePairs.emplace_back("status", std::to_string(static_cast<int>(request_.status())), SQLType::INTEGER);

            std::vector<std::vector<std::string>> record = db_manager->getRecordByCondition(
                "CONTACT_INFO", "id", fmt::format("{}", request_.contact_id())
            );

            if (record.empty()) {
                response.set_operation_status(erp::OperationStatus::failure);
                response.set_updation_status("Contact not found");
                logger->debug("Contact with ID-{} doesn't exist in records", request_.contact_id());
            } else {
                bool deletionSuccess = db_manager->deleteRecord("CONTACT_INFO", condition, error_msg);
                logger->debug("Old record deleted");

                if (!deletionSuccess) {
                    response.set_operation_status(erp::OperationStatus::failure);
                    response.set_updation_status(fmt::format("Error encountered: {}", error_msg));
                    logger->debug("Error in removing contact with ID-{}: {}", request_.contact_id(), error_msg);
                } else {
                    bool insertionSuccess = db_manager->insertRecord("CONTACT_INFO", columnValuePairs, error_msg);
                    if (!insertionSuccess) {
                        response.set_operation_status(erp::OperationStatus::failure);
                        response.set_updation_status(fmt::format("Error encountered: {}", error_msg));
                        logger->debug("Error in inserting updated contact with ID-{}: {}", request_.contact_id(), error_msg);
                    } else {
                        response.set_operation_status(erp::OperationStatus::success);
                        response.set_updation_status("Contact record updated successfully");
                        logger->debug("Contact with ID-{} updated successfully", request_.contact_id());
                    }
                }
            }
        }
        state_ = CallStatus::FINISH;
        responder_.Finish(response, grpc::Status::OK, this);
    } else if (state_ == CallStatus::FINISH) {
        CleanUp();
    }
}

void ContactServiceImpl::UpdateContact::CleanUp() {
    logger->flush();
    delete this;
}




