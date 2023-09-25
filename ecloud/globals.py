# -*- coding: utf-8 -*-
"""
global variables & constants: contains commonly referenced strings & integers
"""
# Author: Jordan Rapp <jrapp7@gatech.edu>
# License: TDG-Attribution-NonCommercial-NoDistrib

from ecloud.scenario_testing.utils.yaml_utils import load_yaml

__version__ = "0.0.3" # 3: CPP server

# CONSTS
class Consts:
    '''
    package constants
    '''

    ECLOUD = "ecloud"
    LOCAL = "local"
    LOCALHOST = "localhost"
    AZURE = "azure"
    CARLA_VERSION = "0.9.12"
    DEFAULT_SCENARIO = "ecloud_town06_config"
    DEFAULT_EDGE_SCENARIO = "ecloud_town06_edge"
    SPECTATOR_INDEX = 0

    # gRPC
    ECLOUD_SERVER_PATH = "./ecloud/ecloud_server/ecloud_server"
    SERVER_PORT = 50051 # gRPC server listens
    PUSH_API_PORT = 50061 # sim orchestrator listens
    PUSH_BASE_PORT = 50101 # client N listens on base + N

    # FILE PATHS
    ENVIRONMENT_CONFIG = "environment_config.yaml" # in eCloud root folder
    # local Carla path
    # eCloud gRPC server path
    # evaluation outputs path(s)

    # EDGE
    WORLD_DT = 0.03 # sec
    EDGE_DT = 0.20 # sec
    EDGE_SEARCH_DT = 2.00 # sec

    # YAML dict keys: server IPs
    CARLA_IP = 'carla_server_public_ip'
    ECLOUD_IP = 'ecloud_server_public_ip'

    # YAML dict keys: clients
    CLIENTS = 'clients'
    CLIENT_MACHINE = 'client_machine'
    MACHINE_NAME = 'machine_name' # self-identifier in ecloud_client to fetch environment vars
    CLIENT_IP = 'vehicle_client_public_ip'
    CLIENT_DNS = 'vehicle_client_dns'

    # random spawning vars
    MIN_DESTINATION_DISTANCE_M = 500
    COLLISION_ERROR = "Spawn failed because of collision at spawn position"

    PLANER_AGENT_STEPS = 12

class EnvironmentConfig():
    '''
    static class containing accessor methods for the environment_config.yaml

    carla_server_public_ip
    ecloud_server_public_ip
    clients:
        client_machine:
            machine_name: ndm
            vehicle_client_public_ip: '20.172.248.156'
            vehicle_client_dns: ''
    '''

    config = load_yaml(Consts.ENVIRONMENT_CONFIG)
    environment = Consts.LOCAL

    @staticmethod
    def set_environment(environment: str) -> None:
        '''
        sets the working environment
        '''
        assert environment in EnvironmentConfig.config
        EnvironmentConfig.environment = environment

    @staticmethod
    def get_environment_params() -> dict:
        '''
        returns params for a given environment
        '''
        return EnvironmentConfig.config[EnvironmentConfig.environment]

    @staticmethod
    def get_carla_ip() -> str:
        '''
        gets the IP of the Carla server
        '''
        return EnvironmentConfig.config[EnvironmentConfig.environment][Consts.CARLA_IP]

    @staticmethod
    def get_ecloud_ip() -> str:
        '''
        gets the IP of gRPC eCloud server
        '''
        return EnvironmentConfig.config[EnvironmentConfig.environment][Consts.ECLOUD_IP]

    @staticmethod
    def get_client_ip_by_name(client_name) -> str:
        '''
        gets the parameters for a given client name - e.g. 'ndm' - so that the client can access its IP config
        '''
        for client_dict in EnvironmentConfig.config[EnvironmentConfig.environment][Consts.CLIENTS].values():
            if client_dict[Consts.MACHINE_NAME] == client_name:
                return client_dict[Consts.CLIENT_IP]

        assert False, f'invalid client name: {client_name}'
