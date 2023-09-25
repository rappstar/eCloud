# -*- coding: utf-8 -*-
"""
Script to run a simulated vehicle
"""
# Authors: Aaron Drysdale <adrysdale3@gatech.edu>
#        : Jordan Rapp <jrapp7@gatech.edu>

import argparse
import sys
import json
import asyncio
import logging

import carla
from google.protobuf.timestamp_pb2 import Timestamp # pylint: disable=no-name-in-module

import ecloud.globals as ecloud_globals
from ecloud.globals import EnvironmentConfig
from ecloud.core.common.cav_world import CavWorld
from ecloud.core.common.vehicle_manager import VehicleManager
from ecloud.core.plan.local_planner_behavior import RoadOption
from ecloud.core.plan.global_route_planner_dao import GlobalRoutePlannerDAO
from ecloud.core.common.ecloud_config import EcloudConfig, DoneBehavior
from ecloud.ecloud_server.ecloud_comms import EcloudClientToServerComms, ecloud_run_push_server
from ecloud.core.application.edge.transform_utils import deserialize_waypoint

import ecloud_pb2 as ecloud

logger = logging.getLogger(ecloud_globals.Consts.ECLOUD)

ECLOUD_PUSH_BASE_PORT = ecloud_globals.Consts.PUSH_BASE_PORT
LOCAL = ecloud_globals.Consts.LOCAL
AZURE = ecloud_globals.Consts.AZURE

def arg_parse():
    '''
    parse input args
    '''
    parser = argparse.ArgumentParser(description="eCloud Vehicle Simulation.")
    parser.add_argument("--apply_ml",
                        action='store_true',
                        help='whether ml/dl framework such as sklearn/pytorch is needed in the testing. '
                             'Set it to true only when you have installed the pytorch/sklearn package.')
    parser.add_argument('-i', "--ipaddress", type=str, default='localhost',
                        help="Specifies the ip address of the server to connect to. [Default: localhost]")
    parser.add_argument('-p', "--port", type=int, default=ecloud_globals.Consts.SERVER_PORT,
                        help=f"Specifies the port to connect to. [Default: {ecloud_globals.Consts.SERVER_PORT}]")
    parser.add_argument('-e', "--environment", type=str, default="local",
                            help="Environment to run in: 'local' or 'azure'. [Default: 'local']")
    parser.add_argument('-m', "--machine", type=str, default="localhost",
                            help="Name of the specific machine name: 'localhost' or 'ndm'. [Default: 'localhost']")
    parser.add_argument('-c',"--container_id", type=int, default=0,
                        help="container ID #. Used as the counter from the base port for the eCloud push service")
    parser.add_argument('-f', "--fatal_errors", action='store_true',
                        help="will raise exceptions when set to allow for easier debugging")

    opt = parser.parse_args()
    return opt

async def main():
    '''
    async main - run Client processes
    '''
    application = ["single"]
    version = "0.9.12"
    tick_id = 0
    reported_done = False
    push_q = asyncio.Queue()

    opt = arg_parse()
    assert opt.environment == LOCAL or opt.environment == AZURE
    EnvironmentConfig.set_environment(opt.environment)
    EcloudConfig.carla_ip = EnvironmentConfig.get_carla_ip()
    EcloudConfig.ecloud_ip = EnvironmentConfig.get_ecloud_ip()
    EcloudConfig.vehicle_ip = EnvironmentConfig.get_client_ip_by_name(opt.machine)

    EcloudConfig.fatal_errors = opt.fatal_errors

    # spawn push server
    port = ECLOUD_PUSH_BASE_PORT + opt.container_id
    push_server = asyncio.create_task(ecloud_run_push_server(port, push_q))

    await asyncio.sleep(1) # pause to let the server spin up

    port = await push_q.get()
    push_q.task_done()

    logger.info("push server spun up on port %s", port)

    ecloud_client = EcloudClientToServerComms(ip_ad=EcloudConfig.ecloud_ip,
                                              port=opt.port)
    ecloud_update = await ecloud_client.send_registration_to_ecloud_server(port)
    vehicle_index = ecloud_update.vehicle_index
    assert vehicle_index is not None

    test_scenario = ecloud_update.test_scenario
    application = ecloud_update.application
    version = ecloud_update.version

    logger.debug("main - application: %s", application)
    logger.debug("main - version: %s", version)

    # create CAV world
    cav_world = CavWorld(opt.apply_ml)

    logger.info("creating VehicleManager vehicle_index: %s", vehicle_index)

    scenario_yaml = json.loads(test_scenario) #load_yaml(test_scenario)
    if 'debug_scenario' in scenario_yaml:
        logger.debug("main - test_scenario: %s", test_scenario) # VERY verbose

    ecloud_config = EcloudConfig(scenario_yaml)
    location_type = ecloud_config.get_location_type()
    done_behavior = ecloud_config.get_done_behavior()

    target_speed = None
    edge_sets_destination = False
    is_edge = ecloud_update.is_edge
    if 'edge_list' in scenario_yaml['scenario']:
        is_edge = True
        # TODO: support multiple edges...
        target_speed = scenario_yaml['scenario']['edge_list'][0]['target_speed']
        edge_sets_destination = scenario_yaml['scenario']['edge_list'][0]['edge_sets_destination'] \
            if 'edge_sets_destination' in scenario_yaml['scenario']['edge_list'][0] else False

    if opt.apply_ml:
        await asyncio.sleep(vehicle_index + 1)

    vehicle_manager = VehicleManager(vehicle_index=vehicle_index,
                                     config_yaml=scenario_yaml,
                                     application=application,
                                     cav_world=cav_world,
                                     carla_version=version,
                                     location_type=location_type,
                                     run_distributed=True,
                                     is_edge=is_edge)

    actor_id = vehicle_manager.vehicle.id
    vid = vehicle_manager.vid

    await ecloud_client.send_carla_data_to_ecloud(vehicle_index, actor_id, vid)

    assert push_q.empty() # currently only process only a single message at a time
    pong = await push_q.get()
    push_q.task_done()

    vehicle_manager.update_info()
    vehicle_manager.set_destination(
                vehicle_manager.vehicle.get_location(),
                vehicle_manager.destination_location,
                clean=True)

    logger.info("vehicle %s beginning scenario tick flow", vehicle_index)
    waypoint_proto = None
    while pong.command != ecloud.Command.END:

        vehicle_update = ecloud.VehicleUpdate()
        if pong.command != ecloud.Command.TICK: # don't print tick message since there are too many
            logger.info("received cmd %s", pong.command)

        # HANDLE DEBUG DATA REQUEST
        if pong.command == ecloud.Command.REQUEST_DEBUG_INFO:
            vehicle_update.vehicle_state = ecloud.VehicleState.DEBUG_INFO_UPDATE
            ecloud_client.serialize_debug_info(vehicle_update, vehicle_manager)

        # HANDLE TICK
        elif pong.command == ecloud.Command.TICK:
            client_start_timestamp = Timestamp()
            client_start_timestamp.GetCurrentTime()
            # update info runs BEFORE waypoint injection
            vehicle_manager.update_info()
            logger.debug("update_info complete")

            if is_edge:
                is_wp_valid = False
                has_not_cleared_buffer = True
                if waypoint_proto is not None:
                    # world = self.vehicle_manager_list[0].vehicle.get_world()
                    # self._dao = GlobalRoutePlannerDAO(world.get_map(), 2)
                    # location = self._dao.get_waypoint(carla.Location(x=car_array[0][i], y=car_array[1][i], z=0.0))
                    world = vehicle_manager.vehicle.get_world()
                    dao = GlobalRoutePlannerDAO(world.get_map(), 2)
                    for swp in waypoint_proto.waypoint_buffer:
                        #logger.debug(swp.SerializeToString())
                        logger.debug("Override Waypoint x: %s, y: %s, z: %s, rl: %s, pt: %s, yw: %s",
                                     swp.transform.location.x,
                                     swp.transform.location.y,
                                     swp.transform.location.z,
                                     swp.transform.rotation.roll,
                                     swp.transform.rotation.pitch,
                                     swp.transform.rotation.yaw)
                        wpt = deserialize_waypoint(swp, dao)
                        logger.debug("DAO Waypoint x: %s, y: %s, z: %s, rl: %s, pt: %s, yw: %s",
                                     wpt.transform.location.x,
                                     wpt.transform.location.y,
                                     wpt.transform.location.z,
                                     wpt.transform.rotation.roll,
                                     wpt.transform.rotation.pitch,
                                     wpt.transform.rotation.yaw)
                        is_wp_valid = vehicle_manager.agent.get_local_planner().is_waypoint_valid(waypoint=wpt)

                        if edge_sets_destination and is_wp_valid:
                            cur_location = vehicle_manager.vehicle.get_location()
                            start_location = carla.Location(x=cur_location.x, y=cur_location.y, z=cur_location.z)
                            end_location = carla.Location(x=wpt.transform.location.x,
                                                          y=wpt.transform.location.y,
                                                          z=wpt.transform.location.z)
                            clean = True # bool(destination["clean"])
                            end_reset = True # bool(destination["reset"])
                            vehicle_manager.set_destination(start_location, end_location, clean, end_reset)

                        elif is_wp_valid:
                            if has_not_cleared_buffer:
                                # override waypoints
                                waypoint_buffer = vehicle_manager.agent.get_local_planner().get_waypoint_buffer()
                                # print(waypoint_buffer)
                                # for waypoints in waypoint_buffer:
                                #   print("Waypoints transform for Vehicle Before Clearing: " + str(i) +
                                # " : ", waypoints[0].transform)
                                waypoint_buffer.clear() #EDIT MADE
                                has_not_cleared_buffer = False
                            waypoint_buffer.append((wpt, RoadOption.STRAIGHT))

                    waypoint_proto = None

                cur_location = vehicle_manager.vehicle.get_location()
                logger.debug("location for vehicle_%s - is - x: %s, y: %s",
                             vehicle_index,
                             cur_location.x,
                             cur_location.y)

                waypoints_buffer_printer = vehicle_manager.agent.get_local_planner().get_waypoint_buffer()
                for waypoints in waypoints_buffer_printer:
                    logger.debug("waypoint_proto: waypoints transform for Vehicle: %s", waypoints[0].transform)

            #waypoints_buffer_printer = vehicle_manager.agent.get_local_planner().get_waypoint_buffer()
            #for waypoints in waypoints_buffer_printer:
            #    logger.warning("final: waypoints transform for Vehicle: %s", waypoints[0].transform)

            should_run_step = False
            if not is_edge or ( has_not_cleared_buffer and waypoint_proto is None ) or \
                    ( ( not has_not_cleared_buffer ) and waypoint_proto is not None ):
                should_run_step = True

            if should_run_step:
                if reported_done:
                    target_speed = 0
                control = vehicle_manager.run_step(target_speed=target_speed)
                logger.debug("run_step complete")

            vehicle_update.tick_id = tick_id

            if should_run_step:
                if control is None or vehicle_manager.is_close_to_scenario_destination():
                    vehicle_update.vehicle_state = ecloud.VehicleState.TICK_DONE
                    if not reported_done:
                        ecloud_client.serialize_debug_info(vehicle_update, vehicle_manager)

                    if control is not None and done_behavior == DoneBehavior.CONTROL:
                        vehicle_manager.apply_control(control)

                else:
                    vehicle_manager.apply_control(control)
                    logger.debug("apply_control complete")

                    step_timestamps = ecloud.Timestamps()
                    step_timestamps.tick_id = tick_id
                    step_timestamps.client_end_tstamp.GetCurrentTime()
                    step_timestamps.client_start_tstamp.CopyFrom(client_start_timestamp)
                    vehicle_manager.debug_helper.update_timestamp(step_timestamps)

                    vehicle_update.vehicle_state = ecloud.VehicleState.TICK_OK
                    vehicle_update.duration_ns = step_timestamps.client_end_tstamp.ToNanoseconds() - \
                                                    step_timestamps.client_start_tstamp.ToNanoseconds()

                if is_edge or vehicle_index == EcloudConfig.SPECTATOR_INDEX:
                    velocity = vehicle_manager.vehicle.get_velocity()
                    prv = ecloud.Velocity()
                    prv.x = velocity.x
                    prv.y = velocity.y
                    prv.z = velocity.z
                    vehicle_update.velocity.CopyFrom(prv)

                    transform = vehicle_manager.vehicle.get_transform()
                    prt = ecloud.Transform()
                    prt.location.x = transform.location.x
                    prt.location.y = transform.location.y
                    prt.location.z = transform.location.z
                    prt.rotation.roll = transform.rotation.roll
                    prt.rotation.yaw = transform.rotation.yaw
                    prt.rotation.pitch = transform.rotation.pitch
                    vehicle_update.transform.CopyFrom(prt)

            else:
                vehicle_update.vehicle_state = ecloud.VehicleState.ERROR # TODO: handle error status
                logger.error("ecloud_client error")

        if not reported_done or done_behavior == DoneBehavior.CONTROL:
            if not reported_done:
                vehicle_update.tick_id = tick_id
                vehicle_update.vehicle_index = vehicle_index
                logger.debug('VEHICLE_UPDATE_DBG: \n vehicle_index: %s \n tick_id: %s \n %s',
                             vehicle_index,
                             tick_id,
                             vehicle_update)
                ecloud_update = await ecloud_client.send_vehicle_update(vehicle_update)

            if vehicle_update.vehicle_state == ecloud.VehicleState.TICK_DONE or \
                    vehicle_update.vehicle_state == ecloud.VehicleState.DEBUG_INFO_UPDATE:
                if vehicle_update.vehicle_state == ecloud.VehicleState.DEBUG_INFO_UPDATE and \
                        pong.command == ecloud.Command.REQUEST_DEBUG_INFO:
                    logger.info("pushed DEBUG_INFO_UPDATE")

                reported_done = True
                logger.info("reported_done")

            assert push_q.empty() # only setup to process a single message
            pong = await push_q.get()
            push_q.task_done()
            assert pong.tick_id != tick_id
            tick_id = pong.tick_id

            if pong.command == ecloud.Command.PULL_WAYPOINTS_AND_TICK:
                wp_request = ecloud.WaypointRequest()
                wp_request.vehicle_index = vehicle_index
                waypoint_proto = await ecloud_client.stub.Client_GetWaypoints(wp_request)
                pong.command = ecloud.Command.TICK

            # HANDLE END
            elif pong.command == ecloud.Command.END:
                logger.critical("END received") # must print for the shell script to detect scenario end
                break

        else: # done
            logger.info("EXIT destroy-on-done vehicle actor")
            break

    # end while
    vehicle_manager.destroy()
    push_server.cancel()
    logger.info("scenario complete. exiting.")
    sys.exit(0)

if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())

    except KeyboardInterrupt:
        logger.info("caught keyboard interrupt")

    except Exception as err: # pylint: disable=broad-exception-caught
        logger.exception("exception hit: %s - %s", type(err), err)
        if EcloudConfig.fatal_errors:
            raise
