# -*- coding: utf-8 -*-
"""
gRPC & general networking communications configuration info for eCloud scenarios
"""
# Author: Jordan Rapp <jrapp7@gatech.edu>
# License: TDG-Attribution-NonCommercial-NoDistrib

import logging
import json
import asyncio
import time
import os

import grpc
import carla

#from ecloud.scenario_testing.utils.yaml_utils import load_yaml
import ecloud.globals as ecloud_globals
from ecloud.core.common.ecloud_config import EcloudConfig

import ecloud_pb2 as ecloud
import ecloud_pb2_grpc as ecloud_rpc

logger = logging.getLogger("ecloud")

NSEC_TO_MSEC = 1/1000000
class EcloudCommsConsts:
    '''
    static class containing comms definitions
    '''
    TIMEOUT_S = 10
    TIMEOUT_MS = TIMEOUT_S * 1000

    RETRY_OPTS = json.dumps({
                    "methodConfig": [
                    {
                        "name": [{"service": "ecloud.Ecloud"}],
                        "retryPolicy": {
                            "maxAttempts": 5,
                            "initialBackoff": "0.05s",
                            "maxBackoff": "0.5s",
                            "backoffMultiplier": 2,
                            "retryableStatusCodes": ["UNAVAILABLE"],
                        },
                    }]})

    GRPC_OPTIONS = [("grpc.lb_policy_name", "pick_first"),
                    ("grpc.enable_retries", 1),
                    ("grpc.keepalive_timeout_ms", TIMEOUT_MS),
                    ("grpc.service_config", RETRY_OPTS)]

class EcloudAPIToServerComms:
    '''
    wrapper class for gRPC comms for Scenario Manager
    '''

    def __init__(self,
                 vehicle_managers,
                 debug_helper,
                 is_edge,
                 push_q,
                 sm_start_tstamp,
                 vehicle_count):

        # references to the parent ScenarioManager member
        self.vehicle_managers = vehicle_managers
        self.debug_helper = debug_helper
        self.push_q = push_q
        self.sm_start_tstamp = sm_start_tstamp

        # set by ScenarioManager
        self.is_edge = is_edge
        self.vehicle_count = vehicle_count

        # actual instance members
        self.client_node_count = 1
        self.tick_id = 0

    async def server_unpack_debug_data(self, stub_) -> None:
        '''
        fetch the protobuf data containing serialized debug data.
        this is in then repopulated into VehicleManagerProxy objects for evaluation
        '''

        logger.info("fetching vehicle debug data")
        vehicle_updates_list = []
        while True:
            ecloud_update = await stub_.Server_GetVehicleUpdates(ecloud.Empty())
            if len(ecloud_update.vehicle_update) == 0:
                break
            for veh in ecloud_update.vehicle_update:
                upd = ecloud.VehicleUpdate()
                upd.CopyFrom(veh)
                vehicle_updates_list.append(upd)
            await asyncio.sleep(0.1)

        #logger.debug(f"{ecloud_update}")
        for vehicle_update in vehicle_updates_list:
            vehicle_manager_proxy = self.vehicle_managers[vehicle_update.vehicle_index]
            vehicle_manager_proxy.localizer.debug_helper.deserialize_debug_info( vehicle_update.loc_debug_helper )
            vehicle_manager_proxy.agent.debug_helper.deserialize_debug_info( vehicle_update.planer_debug_helper )
            vehicle_manager_proxy.debug_helper.deserialize_debug_info(vehicle_update.client_debug_helper)

            network_overhead_by_tick = self.debug_helper.network_time_dict
            overall_steps_by_tick = self.debug_helper.client_tick_time_dict
            for timestamps in vehicle_manager_proxy.debug_helper.timestamps_list:
                if timestamps.tick_id in overall_steps_by_tick:
                    assert timestamps.tick_id in network_overhead_by_tick
                    client_process_time_ms = (timestamps.client_end_tstamp.ToNanoseconds() - \
                                              timestamps.client_start_tstamp.ToNanoseconds()) * NSEC_TO_MSEC # doing work
                    barrier_overhead_time_ms = overall_steps_by_tick[timestamps.tick_id] - \
                                                network_overhead_by_tick[timestamps.tick_id] - client_process_time_ms
                                                # inferred rather than actual "barrier" time

                    # TODO: confirm if we wantt to do this?
                    #if barrier_overhead_time_ms < 0:
                    #    logger.warning(f"got a NEGATIVE inferred barrier_overhead_time value of
                    # {round(barrier_overhead_time_ms, 2)}ms for vehicle {v.vehicle_index}")
                    #barrier_overhead_time_ms = barrier_overhead_time_ms if barrier_overhead_time_ms > 0 else 0

                    self.debug_helper.update_barrier_overhead_time_timestamp(vehicle_manager_proxy.vehicle_index,
                                                                             barrier_overhead_time_ms) # this inferred
                    self.debug_helper.update_client_process_time_timestamp(vehicle_manager_proxy.vehicle_index,
                                                                           client_process_time_ms) # time client was active

                    # dupe the data since it makes evaluation simpler
                    self.debug_helper.update_network_time_per_client_timestamp(vehicle_manager_proxy.vehicle_index,
                                                                               network_overhead_by_tick[timestamps.tick_id])
                    self.debug_helper.update_overall_step_time_per_client_timestamp(vehicle_manager_proxy.vehicle_index,
                                                                                    overall_steps_by_tick[timestamps.tick_id])

                    logger.info('client process time: %sms', round(client_process_time_ms, 2))
                    logger.info('barrier time: %sms', round(barrier_overhead_time_ms, 2))
                    logger.debug("updated time stamp data for vehicle %s", vehicle_manager_proxy.vehicle_index)
                    logger.debug("timestamps: client_end - %s client_start - %s",
                                 timestamps.client_end_tstamp.ToDatetime().time(),
                                 timestamps.client_start_tstamp.ToDatetime().time())

    async def server_unpack_vehicle_updates(self, stub_) -> None:
        '''
        fetch and unpack the vehicle updates.
        used for spectator follow and by Edge implementations that require positional & kinematic data
        '''

        logger.info("fetching vehicle debug data")
        vehicle_updates_list = []
        while True:
            ecloud_update = await stub_.Server_GetVehicleUpdates(ecloud.Empty())
            if len(ecloud_update.vehicle_update) == 0:
                break
            for veh in ecloud_update.vehicle_update:
                upd = ecloud.VehicleUpdate()
                upd.CopyFrom(veh)
                vehicle_updates_list.append(upd)
            await asyncio.sleep(0.1)

        try:
            for vehicle_update in vehicle_updates_list:
                if not vehicle_update.HasField('transform') or not vehicle_update.HasField('velocity'):
                    continue

                if not self.is_edge and vehicle_update.vehicle_index != ecloud_globals.Consts.SPECTATOR_INDEX:
                    continue

                vehicle_manager_proxy = self.vehicle_managers[ vehicle_update.vehicle_index ]
                if hasattr( vehicle_manager_proxy.vehicle, 'is_proxy' ):
                    tfm = carla.Transform(
                    carla.Location(
                        x=vehicle_update.transform.location.x,
                        y=vehicle_update.transform.location.y,
                        z=vehicle_update.transform.location.z),
                    carla.Rotation(
                        yaw=vehicle_update.transform.rotation.yaw,
                        roll=vehicle_update.transform.rotation.roll,
                        pitch=vehicle_update.transform.rotation.pitch))
                    vec = carla.Vector3D(
                        x=vehicle_update.velocity.x,
                        y=vehicle_update.velocity.y,
                        z=vehicle_update.velocity.z)
                    vehicle_manager_proxy.vehicle.set_velocity(vec)
                    vehicle_manager_proxy.vehicle.set_transform(tfm)
        except RuntimeError as r_e:
            logger.exception('failed to properly unpack updates - %s \n\t %s', r_e, vehicle_update)
            if EcloudConfig.fatal_errors:
                raise

        logger.debug("vehicle updates unpacked")

    async def server_push_waypoints(self, stub_, wps_) -> ecloud.Empty:
        '''
        send Edge waypoint data to gRPC server
        '''
        empty = await stub_.Server_PushEdgeWaypoints(wps_)

        return empty

    async def server_do_tick(self, stub_, update_) -> ecloud.Empty:
        '''
        run a simulation step
        '''
        self.tick_id = update_.tick_id

        empty = await stub_.Server_DoTick(update_)

        assert self.push_q.empty() # only process one push at a time
        tick = await self.push_q.get()
        snapshot_t = time.time_ns()
        self.push_q.task_done()

        # the first tick time is dramatically slower due to startup, so we don't want it to skew runtime data
        if self.tick_id == 1:
            self.debug_helper.startup_time_ms = ( snapshot_t - self.sm_start_tstamp.ToNanoseconds() ) * NSEC_TO_MSEC

        else:
            # barrier sync means this is the same for ALL vehicles per tick
            overall_step_time_ms = ( snapshot_t - self.sm_start_tstamp.ToNanoseconds() ) * NSEC_TO_MSEC
            # we care about the worst case per tick - how much did we affect the final vehicle to report.
            # This captures both delay in getting that vehicle started and in it reporting its completion
            step_network_overhead_ms = overall_step_time_ms - ( tick.last_client_duration_ns * NSEC_TO_MSEC )
            # same for all vehicles *per tick*
            self.debug_helper.update_network_time_timestamp(tick.tick_id, step_network_overhead_ms)
            self.debug_helper.update_overall_step_time_timestamp(tick.tick_id, overall_step_time_ms)

            logger.info("timestamps: overall_step_time_ms - %sms | network_overhead_ms - %sms",
                        round(overall_step_time_ms, 2),
                        round(step_network_overhead_ms, 2))

        if update_.command == ecloud.Command.REQUEST_DEBUG_INFO:
            await self.server_unpack_debug_data(stub_)

        else:
            await self.server_unpack_vehicle_updates(stub_)

        return empty

    async def server_start_scenario(self, stub_, update_):
        '''
        starts a given scenario
        '''
        await stub_.Server_StartScenario(update_)

        print(f"start {self.vehicle_count} vehicle containers")

        assert self.push_q.empty()
        tick = await self.push_q.get()
        self.push_q.task_done()

        assert tick.tick_id != 0
        self.client_node_count = tick.tick_id # at startup, we use the tick id to transmit the number of client nodes
        logger.info("scenario running on %s different nodes", self.client_node_count)

        logger.info("vehicle registration complete")

        response = await stub_.Server_GetVehicleUpdates(ecloud.Empty())

        logger.info("vehicle registration data received")

        return response

    async def server_end_scenario(self, stub_):
        '''
        tell the gRPC server to push an END command to all clients
        '''
        empty = await stub_.Server_EndScenario(ecloud.Empty())

        return empty

    def get_node_count(self) -> int:
        '''
        get the node count for this scenario
        '''
        return self.client_node_count

# end EcloudAPIToServerComms

class EcloudClientToServerComms:

    '''
    Wrapper Class around gRPC Vehicle Client Calls
    '''

    def __init__(self, ip_ad: str, port: int) -> None:
        self.channel = grpc.aio.insecure_channel(
                        target=f"{ip_ad}:{port}",
                        options=EcloudCommsConsts.GRPC_OPTIONS)
        self.stub = ecloud_rpc.EcloudStub(self.channel)

    def serialize_debug_info(self, vehicle_update, vehicle_manager) -> None:
        '''
        serialize the debug data from the vehicle manager into a protobuf
        '''
        planer_debug_helper = vehicle_manager.agent.debug_helper
        planer_debug_helper_msg = ecloud.PlanerDebugHelper()
        planer_debug_helper.serialize_debug_info(planer_debug_helper_msg)
        vehicle_update.planer_debug_helper.CopyFrom( planer_debug_helper_msg )

        loc_debug_helper = vehicle_manager.localizer.debug_helper
        loc_debug_helper_msg = ecloud.LocDebugHelper()
        loc_debug_helper.serialize_debug_info(loc_debug_helper_msg)
        vehicle_update.loc_debug_helper.CopyFrom( loc_debug_helper_msg )

        client_debug_helper = vehicle_manager.debug_helper
        client_debug_helper_msg = ecloud.ClientDebugHelper()
        client_debug_helper.serialize_debug_info(client_debug_helper_msg)
        vehicle_update.client_debug_helper.CopyFrom(client_debug_helper_msg)

    async def send_registration_to_ecloud_server(self, port) -> ecloud.SimulationInfo:
        '''
        register this container client with the eCloud server
        '''
        request = ecloud.RegistrationInfo()
        request.vehicle_state = ecloud.VehicleState.REGISTERING
        request.vehicle_port = port
        try:
            request.container_name = os.environ["HOSTNAME"]
        except KeyError:
            request.container_name = f"ecloud_client_{port}.py"

        request.vehicle_ip = EcloudConfig.vehicle_ip

        sim_info = await self.stub.Client_RegisterVehicle(request)

        logger.info("vehicle ID %s received...", sim_info.vehicle_index)

        return sim_info

    async def send_carla_data_to_ecloud(self, vehicle_index, actor_id, vid) -> ecloud.SimulationInfo:
        '''
        send Carla actor data to eCloud server
        '''
        message = {"vehicle_index": vehicle_index, "actor_id": actor_id, "vid": vid}
        logger.info("sending Carla rpc %s", message)

        # send actor ID and vid to API
        update = ecloud.RegistrationInfo()
        update.vehicle_state = ecloud.VehicleState.CARLA_UPDATE
        update.vehicle_index = vehicle_index
        update.vid = vid
        update.actor_id = actor_id

        sim_info = await self.stub.Client_RegisterVehicle(update)

        return sim_info

    async def send_vehicle_update(self, vehicle_update_) -> ecloud.Empty:
        '''
        push a vehicle update message to eCloud server
        '''
        empty = await self.stub.Client_SendUpdate(vehicle_update_)

        return empty

class EcloudPushServer(ecloud_rpc.EcloudServicer):

    '''
    Lightweight gRPC Server Class for Receiving Push Messages from Ochestrator
    '''

    def __init__(self,
                 que: asyncio.Queue):

        logger.info("eCloud push server initialized")
        self.que = que
        self.last_tick = None
        self.last_tick_id = 0
        self.last_tick_command = None
        self.last_tick_last_client_duration_ns = 0
        self.port_no = 0

    def is_dupe(self, tick) -> bool:
        '''
        checks if the current tick is a dupe due to resend
        '''
        if tick.tick_id == self.last_tick_id and \
                tick.command == self.last_tick_command and \
                tick.last_client_duration_ns == self.last_tick_last_client_duration_ns:
            return True

        return False

    def PushTick(self,
                 request: ecloud.Tick,
                 context: grpc.aio.ServicerContext) -> ecloud.Empty:

        tick = request # readability - gRPC prefers overrides preserve variable names
        is_dupe = self.is_dupe(tick)
        if is_dupe:
            logger.warning('received a duplicate tick: had %s | received %s', self.last_tick, tick)
        else:
            self.last_tick = tick
            self.last_tick_id = tick.tick_id
            self.last_tick_command = tick.command
            self.last_tick_last_client_duration_ns = tick.last_client_duration_ns
            logger.info("new tick - %s", tick)

        assert self.que.empty()
        if is_dupe is False:
            self.que.put_nowait(tick)

        return ecloud.Empty()

async def ecloud_run_push_server(port,
                                 que: asyncio.Queue) -> None:
    '''
    runs a simple listen server that accepts event-based message from the central ecloud gRPC server
    '''

    logger.info("spinning up eCloud push server")
    server = grpc.aio.server()
    ecloud_rpc.add_EcloudServicer_to_server(EcloudPushServer(que), server)
    server_started = False
    while not server_started:
        try:
            listen_addr = f"0.0.0.0:{port}"
            server.add_insecure_port(listen_addr)
            server_started = True
        except Exception as port_exception: # pylint: disable=broad-exception-caught
            logger.warning("failed to start push server on port %s - incrementing port & retrying", 
                           port)
            port += 1
            continue

    logger.critical("starting eCloud push server on port %s", port)

    if port >= ecloud_globals.Consts.PUSH_BASE_PORT:
        que.put_nowait(port)

    await server.start()
    await server.wait_for_termination()
