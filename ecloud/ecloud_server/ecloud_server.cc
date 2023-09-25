#include <iostream>
#include <memory>
#include <string>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <cassert>
#include <stdexcept>
#include <errno.h>
#include <csignal>
#include <unistd.h>
#include <chrono>
#include <unordered_map>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_format.h"
#include "absl/log/log.h"
#include "absl/log/flags.h"
#include "absl/log/initialize.h"
#include "absl/log/globals.h"

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include <google/protobuf/util/time_util.h>
#include <google/protobuf/util/json_util.h>

#include "ecloud.grpc.pb.h"
#include "ecloud.pb.h"

#define SPECTATOR_INDEX 0
#define MAX_CARS 512
#define INVALID_TIME 0
#define TICK_ID_INVALID -1

ABSL_FLAG(uint16_t, vehicle_update_batch_size, 32, "Number of vehicle updates to batch at scenario end - keeps from going over gRPC's 4MB limit.");
ABSL_FLAG(uint16_t, ecloud_push_base_port, 50101, "eCloud Client starting port");
ABSL_FLAG(uint16_t, ecloud_push_api_port, 50061, "eCloud Sim API server port");
ABSL_FLAG(uint16_t, port, 50051, "eCloud gRPC server port for the service");
ABSL_FLAG(uint16_t, minloglevel, static_cast<uint16_t>(absl::LogSeverityAtLeast::kInfo),
          "Messages logged at a lower level than this don't actually get logged anywhere");

using google::protobuf::util::TimeUtil;

using grpc::CallbackServerContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerUnaryReactor;
using grpc::Status;

using ecloud::Ecloud;
using ecloud::EcloudResponse;
using ecloud::VehicleUpdate;
using ecloud::Empty;
using ecloud::Tick;
using ecloud::Command;
using ecloud::VehicleState;
using ecloud::SimulationInfo;
using ecloud::RegistrationInfo;
using ecloud::WaypointBuffer;
using ecloud::Waypoint;
using ecloud::Transform;
using ecloud::Location;
using ecloud::Rotation;
using ecloud::LocDebugHelper;
using ecloud::AgentDebugHelper;
using ecloud::PlanerDebugHelper;
using ecloud::ClientDebugHelper;
using ecloud::Timestamps;
using ecloud::WaypointRequest;
using ecloud::EdgeWaypoints;

volatile std::atomic<int16_t> numCompletedVehicles_;
volatile std::atomic<int16_t> numRepliedVehicles_;
volatile std::atomic<int32_t> tickId_;

// for debugging slow/non-responsive containers
#ifdef _DEBUG
bool repliedCars_[MAX_CARS];
std::string carNames_[MAX_CARS]; 
#endif

bool isEdge_;
int16_t numCars_;
std::string configYaml_;
std::string application_;
std::string version_;

std::string simIP_;

VehicleState vehState_;
Command command_;

std::vector<std::pair<int16_t, std::string>> serializedEdgeWaypoints_; // vehicleIdx, serializedWPBuffer
std::unordered_map<int16_t, std::string> pendingReplies_; // vehIdx --> serializedProto: serializing allows messages of differing types

// at startup, it's critical that we only register individual clients once and count nodes properly
// on subsequent ticks, the hashmap protects against repeat messages 
absl::Mutex mu_;

volatile std::atomic<int8_t> nodeCount_ ABSL_GUARDED_BY(mu_);
volatile std::atomic<int16_t> numRegisteredVehicles_ ABSL_GUARDED_BY(mu_);
std::vector<std::string> clientNodes_ ABSL_GUARDED_BY(mu_);

class PushClient
{
    public:
        explicit PushClient( std::shared_ptr<grpc::Channel> channel, std::string connection ) :
                            stub_(Ecloud::NewStub(channel)), connection_(connection) {}

        bool PushTick(int32_t tickId, Command command, int64_t lastClientDurationNS)
        {
            Tick tick;
            tick.set_tick_id(tickId);
            tick.set_command(command);

            LOG_IF(INFO, command == Command::END) << "pushing END";

            tick.set_last_client_duration_ns(lastClientDurationNS);

            grpc::ClientContext context;
            Empty empty;

            std::mutex mu;
            std::condition_variable cv;
            bool done = false;
            Status status;
            stub_->async()->PushTick(&context, &tick, &empty,
                            [&mu, &cv, &done, &status](Status s) {
                            status = std::move(s);
                            std::lock_guard<std::mutex> lock(mu);
                            done = true;
                            cv.notify_one();
                            });

            std::unique_lock<std::mutex> lock(mu);
            while (!done) {
                cv.wait(lock);
            }

            if (status.ok()) {
                return true;
            } else {
                LOG(ERROR) << status.error_code() << ": " << status.error_message();
                return false;
            }
        }

    private:
        std::unique_ptr<Ecloud::Stub> stub_;
        std::string connection_;
};

// Logic and data behind the server's behavior.
class EcloudServiceImpl final : public Ecloud::CallbackService {
public:
    explicit EcloudServiceImpl() {
        numCompletedVehicles_.store(0);
        numRepliedVehicles_.store(0);
        numRegisteredVehicles_.store(0);
        tickId_.store(0);
        nodeCount_.store(0);

        vehState_ = VehicleState::REGISTERING;
        command_ = Command::TICK;

        numCars_ = 0;
        configYaml_ = "";
        isEdge_ = false;

        simIP_ = "localhost";

        const std::string connection = absl::StrFormat("%s:%d", simIP_, absl::GetFlag(FLAGS_ecloud_push_api_port));
        simAPIClient_ = new PushClient(grpc::CreateChannel(connection, grpc::InsecureChannelCredentials()), connection);

        vehicleClients_.clear();
        pendingReplies_.clear();
    }

    ServerUnaryReactor* Server_GetVehicleUpdates(CallbackServerContext* context,
                               const Empty* empty,
                               EcloudResponse* reply) override {

        DLOG(INFO) << "Server_GetVehicleUpdates - deserializing updates.";

        static int16_t k_replyVehIdx = 0;
        do
        {
            assert( pendingReplies_.find(k_replyVehIdx) != pendingReplies_.end() );
            
            const std::string msg = pendingReplies_.at(k_replyVehIdx);
            if( msg.c_str()[0] != '\0' )
            {
                VehicleUpdate *update = reply->add_vehicle_update();
                update->ParseFromString(msg);
                pendingReplies_.at(k_replyVehIdx) = "";

                DLOG(INFO) << "update: vehicle_index - " << update->vehicle_index();
            }
            k_replyVehIdx++;

        } while ( ( k_replyVehIdx < pendingReplies_.size() ) && ( k_replyVehIdx % absl::GetFlag(FLAGS_vehicle_update_batch_size) != 0 ) );

        DLOG(INFO) << "Server_GetVehicleUpdates - updates deserialized up to vehicle index " << k_replyVehIdx;

        if ( k_replyVehIdx == numCars_ )
        {
            numRepliedVehicles_ = 0;
            k_replyVehIdx = 0;
        }
    
        ServerUnaryReactor* reactor = context->DefaultReactor();
        reactor->Finish(Status::OK);
        return reactor;
    }

    ServerUnaryReactor* Client_SendUpdate(CallbackServerContext* context,
                               const VehicleUpdate* request,
                               Empty* empty) override {
        /* interesting options here:
         * - we could enforce a maximum time limit for individual vehicles to respond. it's not clear the sim breaks if we miss a tick
         * - we could enforce a minumum % completion limit for invidual vehicle responses.
         */
        const int16_t vIdx = request->vehicle_index();
        LOG_IF(ERROR, pendingReplies_.at(vIdx).c_str()[0] != '\0') << "Client_SendUpdate received a reply for vehicle " << vIdx << " that already had a pending reply stored";
        // has not proven necessary, but we can enforce uniqueness of replies with: if ( pendingReplies_.at(vIdx).c_str()[0] == '\0' )
        const VehicleState vState = request->vehicle_state();
        bool storeAll = isEdge_ || vState == VehicleState::TICK_DONE || vState == VehicleState::DEBUG_INFO_UPDATE;
        if ( storeAll || vIdx == SPECTATOR_INDEX )
        {
            assert( pendingReplies_.find(vIdx) != pendingReplies_.end() );
            assert( storeAll || ( !storeAll && vIdx == SPECTATOR_INDEX ) );
            assert( ( request->tick_id() > 0 && request->tick_id() == tickId_.load() ) || request->tick_id() <= 0 );

            std::string msg;
            request->SerializeToString(&msg);
            pendingReplies_.at(vIdx) = msg;
        }

#ifdef _DEBUG
        repliedCars_[vIdx] = true;
#endif

        DLOG(INFO) << "Client_SendUpdate - received reply from vehicle " << vIdx << " for tick id:" << request->tick_id();

        if ( vState == VehicleState::TICK_DONE )
        {
            numCompletedVehicles_++;
            DLOG(INFO) << "Client_SendUpdate - TICK_DONE - tick id: " << tickId_ << " vehicle id: " << vIdx;
        }
        else if ( vState == VehicleState::TICK_OK )
        {
            numRepliedVehicles_++;
        }
        else if ( vState == VehicleState::DEBUG_INFO_UPDATE )
        {
            numCompletedVehicles_++;
            DLOG(INFO) << "Client_SendUpdate - DEBUG_INFO_UPDATE - tick id: " << tickId_ << " vehicle id: " << request->vehicle_index();
        }

        // BEGIN PUSH
        const int16_t replies_ = numRepliedVehicles_.load();
        const int16_t completions_ = numCompletedVehicles_.load();
        const bool complete_ = ( replies_ + completions_ ) == numCars_;

        LOG_IF(INFO, complete_ ) << "tick " << request->tick_id() << " COMPLETE";
        if ( complete_ )
        {
            const int64_t lastClientDurationNS = request->duration_ns();
            simAPIClient_->PushTick( request->tick_id(), command_, lastClientDurationNS );
        }

        ServerUnaryReactor* reactor = context->DefaultReactor();
        reactor->Finish(Status::OK);
        return reactor;
    }

    // server can push WP *before* ticking world and client can fetch them before it ticks
    ServerUnaryReactor* Client_GetWaypoints(CallbackServerContext* context,
                               const WaypointRequest* request,
                               WaypointBuffer* buffer) override {

        for ( int i = 0; i < serializedEdgeWaypoints_.size(); i++ )
        {
            const std::pair<int16_t, std::string > wpPair = serializedEdgeWaypoints_[i];
            if ( wpPair.first == request->vehicle_index() )
            {
                buffer->set_vehicle_index(request->vehicle_index());
                WaypointBuffer *wpBuf;
                wpBuf->ParseFromString(wpPair.second);
                for ( Waypoint wp : wpBuf->waypoint_buffer())
                {
                    Waypoint *p = buffer->add_waypoint_buffer();
                    p->CopyFrom(wp);
                }
                break;
            }
        }

        ServerUnaryReactor* reactor = context->DefaultReactor();
        reactor->Finish(Status::OK);
        return reactor;
    }

    ServerUnaryReactor* Client_RegisterVehicle(CallbackServerContext* context,
                               const RegistrationInfo* request,
                               SimulationInfo* reply) override {

        assert( configYaml_ != "" );

        if ( request->vehicle_state() == VehicleState::REGISTERING )
        {
            DLOG(INFO) << "got a registration update";

            mu_.Lock();
            const int16_t vIdx = numRegisteredVehicles_.load();
            reply->set_vehicle_index(vIdx);
            const std::string connection = absl::StrFormat("%s:%d", request->vehicle_ip(), request->vehicle_port());
            const std::string ip = request->vehicle_ip();
            if ( std::find( clientNodes_.begin(), clientNodes_.end(), ip ) == clientNodes_.end() )
            {    
                nodeCount_++;
                clientNodes_.push_back(ip);
            }
            PushClient *vehicleClient = new PushClient(grpc::CreateChannel(connection, grpc::InsecureChannelCredentials()), connection);
            vehicleClients_.push_back(std::move(vehicleClient));
            pendingReplies_.insert(std::make_pair(vIdx, ""));
            numRegisteredVehicles_++;
            mu_.Unlock();

            reply->set_test_scenario(configYaml_);
            reply->set_application(application_);
            reply->set_version(version_);

            DLOG(INFO) << "RegisterVehicle - REGISTERING - container " << request->container_name() << " got vehicle id: " << reply->vehicle_index();

#ifdef _DEBUG
            carNames_[reply->vehicle_index()] = request->container_name();
#endif
        }
        else if ( request->vehicle_state() == VehicleState::CARLA_UPDATE )
        {
            const int16_t vIdx = request->vehicle_index();
            reply->set_vehicle_index(vIdx);

            DLOG(INFO) << "RegisterVehicle - CARLA_UPDATE - vehicle_index: " << vIdx << " | actor_id: " << request->actor_id() << " | vid: " << request->vid();

            std::string msg = pendingReplies_.at(vIdx);
            LOG_IF(INFO, msg.c_str()[0] != '\0') << vIdx << " had stored message: " << msg;
            if( msg.c_str()[0] == '\0' )
            {
                request->SerializeToString(&msg);
                pendingReplies_.at(vIdx) = msg;
                numRepliedVehicles_++;
            }
        }
        else
        {
            assert(false);
        }

        if ( numRegisteredVehicles_ < numCars_ )
        {
            LOG(INFO) << "received " << numRegisteredVehicles_.load() << " registrations";
        }
        else
        {
            const int16_t replies_ = numRepliedVehicles_.load();
            LOG(INFO) << "received " << replies_ << " replies";
        
            const bool complete_ = ( replies_ == numCars_ );
            LOG_IF(INFO, complete_ ) << "REGISTRATION COMPLETE";
            if ( complete_ )
            {
                assert( vehState_ == VehicleState::REGISTERING && replies_ == pendingReplies_.size() );
                simAPIClient_->PushTick( nodeCount_.load(), command_, INVALID_TIME);
            }
        }

        ServerUnaryReactor* reactor = context->DefaultReactor();
        reactor->Finish(Status::OK);
        return reactor;
    }

    ServerUnaryReactor* Server_DoTick(CallbackServerContext* context,
                               const Tick* request,
                               Empty* empty) override {
#ifdef _DEBUG
        for ( int i = 0; i < numCars_; i++ )
            repliedCars_[i] = false;
#endif

        numRepliedVehicles_ = 0;
        assert(tickId_ == request->tick_id() - 1);
        tickId_++;
        command_ = request->command();

        const auto now = std::chrono::system_clock::now();
        DLOG(INFO) << "received new tick " << request->tick_id() << " at " << std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()).count();

        const int32_t tickId = request->tick_id();
        for ( int i = 0; i < vehicleClients_.size(); i++ )
        {
            PushClient *v = vehicleClients_[i];
            std::thread t( &PushClient::PushTick, v, tickId, command_, INVALID_TIME );
            t.detach();
        }

        ServerUnaryReactor* reactor = context->DefaultReactor();
        reactor->Finish(Status::OK);
        return reactor;
    }

    ServerUnaryReactor* Server_PushEdgeWaypoints(CallbackServerContext* context,
                               const EdgeWaypoints* edgeWaypoints,
                               Empty* empty) override {
        serializedEdgeWaypoints_.clear();

        for ( WaypointBuffer wpBuf : edgeWaypoints->all_waypoint_buffers() )
        {   std::string serializedWPs;
            wpBuf.SerializeToString(&serializedWPs);
            const std::pair< int16_t, std::string > wpPair = std::make_pair( wpBuf.vehicle_index(), serializedWPs );
            serializedEdgeWaypoints_.push_back(wpPair);
        }

        ServerUnaryReactor* reactor = context->DefaultReactor();
        reactor->Finish(Status::OK);
        return reactor;
    }

    ServerUnaryReactor* Server_StartScenario(CallbackServerContext* context,
                               const SimulationInfo* request,
                               Empty* empty) override {
        vehState_ = VehicleState::REGISTERING;

        configYaml_ = request->test_scenario();
        application_ = request->application();
        version_ = request->version();
        numCars_ = request->vehicle_index(); // bit of a hack to use vindex as count
        isEdge_ = request->is_edge();
        // TODO: simIP_ = // always localhost for now

        assert( numCars_ <= MAX_CARS );
        DLOG(INFO) << "numCars_: " << numCars_;

        ServerUnaryReactor* reactor = context->DefaultReactor();
        reactor->Finish(Status::OK);
        return reactor;
    }

    ServerUnaryReactor* Server_EndScenario(CallbackServerContext* context,
                               const Empty* request,
                               Empty* reply) override {
        command_ = Command::END;

        LOG(INFO) << "pushing END";
        for ( int i = 0; i < vehicleClients_.size(); i++ )
            vehicleClients_[i]->PushTick(TICK_ID_INVALID, Command::END, INVALID_TIME); // don't thread --> block

        ServerUnaryReactor* reactor = context->DefaultReactor();
        reactor->Finish(Status::OK);
        return reactor;
    }

    private:

        std::vector<PushClient*> vehicleClients_ ABSL_GUARDED_BY(mu_);
        PushClient *simAPIClient_;
};

void RunServer(uint16_t port) {
    EcloudServiceImpl service;

    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    const std::string server_address = absl::StrFormat("0.0.0.0:%d", port );
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    std::cout << "server listening on port " << port << std::endl;
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Sample way of setting keepalive arguments on the server. Here, we are
    // configuring the server to send keepalive pings at a period of 10 minutes
    // with a timeout of 20 seconds. Additionally, pings will be sent even if
    // there are no calls in flight on an active HTTP2 connection. When receiving
    // pings, the server will permit pings at an interval of 10 seconds.
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS,
                                10 * 60 * 1000 /*10 min*/);
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS,
                                20 * 1000 /*20 sec*/);
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
    builder.AddChannelArgument(
        GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS,
        10 * 1000 /*10 sec*/);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char* argv[]) {

    // 2 - std::cout << "ABSL: ERROR - " << static_cast<uint16_t>(absl::LogSeverityAtLeast::kError) << std::endl;
    // 1 - std::cout << "ABSL: WARNING - " << static_cast<uint16_t>(absl::LogSeverityAtLeast::kWarning) << std::endl;
    // 0 - std::cout << "ABSL: INFO - " << static_cast<uint16_t>(absl::LogSeverityAtLeast::kInfo) << std::endl;

    absl::ParseCommandLine(argc, argv);
    //absl::InitializeLog();

    std::thread server = std::thread(&RunServer,absl::GetFlag(FLAGS_port));
    
    absl::SetMinLogLevel(static_cast<absl::LogSeverityAtLeast>(absl::GetFlag(FLAGS_minloglevel)));

    server.join();

    return 0;
}
