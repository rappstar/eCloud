# -*- coding: utf-8 -*-
"""
eCloud
---------
default Town 06 Scenario for **Edge**

derives configuration from command line overrides & YAML

DO NOT USE for 2-Lane Free
"""
# Author: Jordan Rapp, Dean Blank, Tyler Landle <Georgia Tech>
# License: TDG-Attribution-NonCommercial-NoDistrib

# Core
import os
import time
import asyncio
import logging

# 3rd Party
import carla

# OpenCDA Utils
import ecloud.scenario_testing.utils.sim_api as sim_api
from ecloud.scenario_testing.utils.yaml_utils import load_yaml
from ecloud.core.common.cav_world import CavWorld
from ecloud.scenario_testing.evaluations.evaluate_manager import \
    EvaluationManager
# ONLY *required* for 2 Lane highway scenarios
# import ecloud.scenario_testing.utils.customized_map_api as map_api

from ecloud.core.common.ecloud_config import EcloudConfig

import ecloud_pb2 as ecloud

# Consts
LOG_NAME = f"{os.path.basename(__file__)}.log"
SCENARIO_NAME = f"{os.path.basename(__file__)}"
TOWN = 'Town06'

logger = logging.getLogger("ecloud")

def run_scenario(opt, config_yaml):
    '''
    scenario runner for default eCloud scenario on Carla's Town 06
    '''
    step_count = 0
    step = 0

    assert opt.distributed == 1
    run_distributed = True # Edge must run distributed

    scenario_params = load_yaml(config_yaml)

    assert 'ecloud' in scenario_params['scenario']
    ecloud_config = EcloudConfig(scenario_params)
    ecloud_config.set_fatal_errors(opt.fatal_errors)
    step_count = ecloud_config.get_step_count() if opt.steps == 0 else opt.steps

    # sanity checks...
    assert 'edge_list' in scenario_params['scenario'] # must be present to use this template for edge scenarios
    assert 'sync_mode' in scenario_params['world'] and scenario_params['world']['sync_mode'] is True
    assert scenario_params['world']['fixed_delta_seconds'] == 0.03 \
            or scenario_params['world']['fixed_delta_seconds'] == 0.05

    # spectator configs
    world_x = scenario_params['world']['x_pos'] if 'x_pos' in scenario_params['world'] else 0
    world_y = scenario_params['world']['y_pos'] if 'y_pos' in scenario_params['world'] else 0
    world_z = scenario_params['world']['z_pos'] if 'z_pos' in scenario_params['world'] else 256
    world_roll = scenario_params['world']['roll'] if 'roll' in scenario_params['world'] else 0
    world_pitch = scenario_params['world']['pitch'] if 'pitch' in scenario_params['world'] else -90
    world_yaw = scenario_params['world']['yaw'] if 'yaw' in scenario_params['world'] else 0

    cav_world = CavWorld(opt.apply_ml)
    # create scenario manager
    scenario_manager = sim_api.ScenarioManager(scenario_params,
                                                opt.apply_ml,
                                                opt.version,
                                                town=TOWN,
                                                cav_world=cav_world,
                                                distributed=run_distributed,
                                                log_level=opt.log_level,
                                                ecloud_config=ecloud_config,
                                                run_carla=opt.run_carla)
    
    scenario_manager.init_networking() # TODO: add state variable that comms init'ed & assert

    if opt.record:
        scenario_manager.client. \
            start_recorder(LOG_NAME, True)

    world_dt = scenario_params['world']['fixed_delta_seconds']
    edge_dt = scenario_params['edge_base']['edge_dt']
    assert edge_dt % world_dt == 0, 'edge time must be an exact multiple of world time'

    # create single cavs
    edge_list = \
        scenario_manager.create_edge_manager(application='edge',
                                             edge_dt=edge_dt,
                                             world_dt=world_dt)

    # create background traffic in carla
    #traffic_manager, bg_veh_list = \
        #scenario_manager.create_traffic_carla()

    eval_manager = \
        EvaluationManager(scenario_manager.cav_world,
                            script_name='ecloud_edge_scenario',
                            current_time=scenario_params['current_time'])

    spectator = scenario_manager.world.get_spectator()
    spectator_vehicle = edge_list[0].vehicle_manager_list[0].vehicle

    # run steps
    step = 0
    flag = True
    waypoint_buffer = []
    world_time = 0

    # execute scenario
    try:
        flag = True
        while flag:
            print(f"ticking - step: {step}")
            scenario_manager.tick_world()
            world_time += world_dt

            if world_time % edge_dt == 0:
                world_time = 0

                waypoint_buffer.clear()
                for edge in edge_list:
                    edge.update_information()
                    waypoint_buffer = edge.run_step()

                scenario_manager.push_waypoint_buffer(waypoint_buffer)
                flag = scenario_manager.broadcast_message(ecloud.Command.PULL_WAYPOINTS_AND_TICK)

            else:
                flag = scenario_manager.broadcast_tick()

            # only required for specate
            transform = spectator_vehicle.get_transform()
            spectator.set_transform(carla.Transform(
                transform.location +
                carla.Location(
                    x=world_x,
                    y=world_y,
                    z=world_z),
                carla.Rotation(
                    yaw=world_yaw,
                    roll=world_roll,
                    pitch=world_pitch)))

            step = step + 1
            if step > step_count:
                if run_distributed:
                    flag = scenario_manager.broadcast_message(ecloud.Command.REQUEST_DEBUG_INFO)
                break

    except RuntimeError as scenario_error:
        logger.exception("runtime error hit during scenario execution: %s - %s", type(scenario_error), scenario_error)
        if opt.fatal_errors:
            raise

    else:
        if run_distributed:
            scenario_manager.end() # only dist requires explicit scenario end call

        if step > step_count:
            eval_manager.evaluate()

        if opt.record:
            scenario_manager.client.stop_recorder()

    finally:

        # for edge in edge_list:
        #     edge.destroy()

        scenario_manager.close()
