# -*- coding: utf-8 -*-
"""
Basic class of CAV
"""
# Author: Runsheng Xu <rxx3386@ucla.edu>
# License: TDG-Attribution-NonCommercial-NoDistrib

import uuid
import opencda.logging_ecloud
import logging
import time
from opencda.core.actuation.control_manager \
    import ControlManager
from opencda.core.application.platooning.platoon_behavior_agent\
    import PlatooningBehaviorAgent
from opencda.core.common.v2x_manager \
    import V2XManager
from opencda.core.sensing.localization.localization_manager \
    import LocalizationManager
from opencda.core.sensing.perception.perception_manager \
    import PerceptionManager
from opencda.core.plan.behavior_agent \
    import BehaviorAgent
from opencda.core.common.data_dumper import DataDumper


class VehicleManager(object):
    """
    A class manager to embed different modules with vehicle together.

    Parameters
    ----------
    vehicle : carla.Vehicle
        The carla.Vehicle. We need this class to spawn our gnss and imu sensor.

    config_yaml : dict
        The configuration dictionary of the localization module.

    application : list
        The application category, currently support:['single','platoon'].

    carla_map : carla.Map
        The CARLA simulation map.

    cav_world : opencda object
        CAV World.

    current_time : str
        Timestamp of the simulation beginning.

    data_dumping : bool
        Indicates whether to dump sensor data during simulation.

    Attributes
    ----------
    v2x_manager : opencda object
        The current V2X manager.

    localizer : opencda object
        The current localization manager.

    perception_manager : opencda object
        The current V2X perception manager.

    agent : opencda object
        The current carla agent that handles the basic behavior
         planning of ego vehicle.

    controller : opencda object
        The current control manager.

    data_dumper : opencda object
        Used for dumping sensor data.
    """

    def __init__(
            self,
            vehicle,
            config_yaml,
            application,
            carla_map,
            cav_world,
            current_time='',
            data_dumping=False):

        # an unique uuid for this vehicle
        self.vid = str(uuid.uuid1())
        self.vehicle = vehicle
        self.carla_map = carla_map

        # retrieve the configure for different modules
        sensing_config = config_yaml['sensing']
        behavior_config = config_yaml['behavior']
        control_config = config_yaml['controller']
        v2x_config = config_yaml['v2x']

        # v2x module
        self.v2x_manager = V2XManager(cav_world, v2x_config, self.vid)
        # localization module
        self.localizer = LocalizationManager(
            vehicle, sensing_config['localization'], carla_map)
        # perception module
        self.perception_manager = PerceptionManager(
            vehicle, sensing_config['perception'], cav_world,
            data_dumping)

        # behavior agent
        self.agent = None
        if 'platooning' in application:
            platoon_config = config_yaml['platoon']
            self.agent = PlatooningBehaviorAgent(
                vehicle,
                self,
                self.v2x_manager,
                behavior_config,
                platoon_config,
                carla_map)
        else:
            self.agent = BehaviorAgent(vehicle, carla_map, behavior_config)

        # Control module
        self.controller = ControlManager(control_config)

        if data_dumping:
            self.data_dumper = DataDumper(self.perception_manager,
                                          vehicle.id,
                                          save_time=current_time)
        else:
            self.data_dumper = None

        cav_world.update_vehicle_manager(self)

    def set_destination(
            self,
            start_location,
            end_location,
            clean=False,
            end_reset=True):
        """
        Set global route.

        Parameters
        ----------
        start_location : carla.location
            The CAV start location.

        end_location : carla.location
            The CAV destination.

        clean : bool
             Indicator of whether clean waypoint queue.

        end_reset : bool
            Indicator of whether reset the end location.

        Returns
        -------
        """

        self.agent.set_destination(
            start_location, end_location, clean, end_reset)

    def update_info(self):
        """
        Call perception and localization module to
        retrieve surrounding info an ego position.
        """
        # localization
        start_time = time.time()
        self.localizer.localize()
        end_time = time.time()
        logging.debug("Localizer time: %s" %(end_time - start_time))

        start_time = time.time()
        ego_pos = self.localizer.get_ego_pos()
        ego_spd = self.localizer.get_ego_spd()
        end_time = time.time()
        logging.debug("Localizer time: %s" %(end_time - start_time)) 

        # object detection
        start_time = time.time()
        objects = self.perception_manager.detect(ego_pos)
        end_time = time.time()
        logging.debug("Perception time: %s" %(end_time - start_time)) 

        # update ego position and speed to v2x manager,
        # and then v2x manager will search the nearby cavs
        start_time = time.time()
        self.v2x_manager.update_info(ego_pos, ego_spd)
        end_time = time.time()
        logging.debug("v2x manager update info time: %s" %(end_time - start_time)) 

        start_time = time.time()
        self.agent.update_information(ego_pos, ego_spd, objects)
        end_time = time.time()
        logging.debug("Agent Update info time: %s" %(end_time - start_time)) 
        # pass position and speed info to controller
        start_time = time.time()
        self.controller.update_info(ego_pos, ego_spd)
        end_time = time.time()
        logging.debug("Controller update time: %s" %(end_time - start_time)) 

    def run_step(self, target_speed=None):
        """
        Execute one step of navigation.
        """
        

        pre_vehicle_step_time = time.time()
        target_speed, target_pos = self.agent.run_step(target_speed)
        end_time = time.time()
        logging.debug("Agent step time: %s" %(end_time - pre_vehicle_step_time))
        control = self.controller.run_step(target_speed, target_pos)
        post_vehicle_step_time = time.time()
        logging.debug("Controller step time: %s" %(post_vehicle_step_time - end_time))
        logging.debug("Vehicle step time: %s" %(post_vehicle_step_time - pre_vehicle_step_time))        
 
        # dump data
        if self.data_dumper:
            self.data_dumper.run_step(self.perception_manager,
                                      self.localizer,
                                      self.agent)

        return control

    def destroy(self):
        """
        Destroy the actor vehicle
        """
        self.perception_manager.destroy()
        self.localizer.destroy()
        self.vehicle.destroy()
