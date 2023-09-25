# -*- coding: utf-8 -*-
"""
Script to run different scenarios.
"""
# Author: Runsheng Xu <rxx3386@ucla.edu>
#       : Jordan Rapp <jrapp7@gatech.edu>
# License: TDG-Attribution-NonCommercial-NoDistrib

import argparse
import importlib
import os
import sys
import subprocess
import logging
import re

from ecloud.globals import __version__, EnvironmentConfig
import ecloud.globals as ecloud_globals

logger = logging.getLogger(ecloud_globals.Consts.ECLOUD)

import_module = re.compile(r'import ([\.A-Za-z0-9_-]+) ')
import_class = re.compile(r'from ([\.A-Za-z0-9_-]+) import')

def arg_parse():
    '''
    fetch the command line args & returns an args object
    '''
    parser = argparse.ArgumentParser(description="eCloudSim scenario runner.")
    parser.add_argument('-t', "--test_scenario", type=str, default=ecloud_globals.Consts.DEFAULT_SCENARIO,
                        help='Define the name of the scenario you want to test. The given name must'
                             'match one of the testing scripts(e.g. single_2lanefree_carla) in '
                             'ecloud/scenario_testing/ folder'
                             ' as well as the corresponding yaml file in ecloud/scenario_testing/config_yaml.'
                             f'[Default: {ecloud_globals.Consts.DEFAULT_SCENARIO}]')

    # CONFIGURATION ARGS
    parser.add_argument('-n', "--num_cars", type=int, default=0,
                            help="number of vehicles to run - forces RANDOM spawning behavior")
    parser.add_argument('-d', "--distributed", type=int, default=1,
                            help="run a distributed scenario.")
    parser.add_argument('-l', "--log_level", type=int, default=0,
                            help="0: DEBUG | 1: INFO | WARNING: 2 | ERROR: 3")
    parser.add_argument('-b', "--build", action="store_true",
                            help="Rebuild gRPC proto files")
    parser.add_argument('-s', "--steps", type=int, default=0,
                            help="Number of scenario ticks to execute before exiting; if set, overrides scenario config")
    parser.add_argument('-e', "--environment", type=str, default=ecloud_globals.Consts.LOCAL,
                            help=f"Environment to run in: 'local' or 'azure'. [Default: '{ecloud_globals.Consts.LOCAL}']")
    parser.add_argument('-r', "--run_carla", type=str, nargs='?', default=False, const=" ",
                            help="Run Carla with optional args; use = --run_carla='-RenderOffscreen'")
    parser.add_argument('-f', "--fatal_errors", action='store_true',
                        help="will raise exceptions when set to allow for easier debugging")
    parser.add_argument('-g', "--edge", action='store_true',
                        help="run the default Edge scenario on Town 06. MUST specify num_cars")

    # SEQUENTIAL ONLY
    parser.add_argument("--apply_ml",
                        action='store_true',
                        help='whether ml/dl framework such as sklearn/pytorch is needed in the testing. '
                             'Set it to true only when you have installed the pytorch/sklearn package.'
                             'NOT compatible with distributed scenarios:'
                             'containers must be started at runtime with perception enabled.')

    # DEPRECATED
    parser.add_argument("--record", action='store_true', help='whether to record and save the simulation process to'
                                                              '.log file')
    parser.add_argument("--version", type=str, default="0.9.12",
                            help="Carla version. [default: 0.9.12]") # only support version 0.9.12
    opt = parser.parse_args()
    return opt

# TODO: move to new Util class
def check_imports():
    '''
    debug helper function to scan for missing imports
    '''
    missing_imports = {}
    for (root,_,files) in os.walk(ecloud_globals.Consts.ECLOUD, topdown=True):
        for file in files:
            if file.endswith('.py'):
                # print(f"{file}")
                with open(os.path.join(root, file), 'r', encoding='utf-8') as handle:
                    lines = handle.read()
                    for line in lines.splitlines():
                        rex = re.search(import_module, line)
                        if rex:
                            module = rex.group(1)
                            try:
                                importlib.import_module(module)
                            except ModuleNotFoundError as module_error:
                                if module not in missing_imports and f"{module_error}" not in missing_imports.values():
                                    missing_imports[module] = f"{module_error}"
                                    logger.error("failed importing %s for file %s - %s", module, file, module_error)
                                continue
                            else:
                                logger.debug("module %s imported OK", module)

                        rex = re.search(import_class, line)
                        if rex:
                            try:
                                module = rex.group(1)
                                importlib.import_module(module)
                            except ModuleNotFoundError as class_error:
                                if module not in missing_imports and f"{class_error}" not in missing_imports.values():
                                    missing_imports[module] = f"{class_error}"
                                    logger.error("failed importing %s for file %s - %s", module, file, class_error)
                                continue
                            else:
                                logger.debug("module %s imported OK", module)

def get_scenario(opt):
    '''
    fetch the desired scenario module & associatd YAML
    '''
    assert '.py' not in opt.test_scenario

    testing_scenario = None
    config_yaml = None
    error = None
    try:
        testing_scenario = importlib.import_module(f"ecloud.scenario_testing.{opt.test_scenario}")
    except ModuleNotFoundError:
        error = f"{opt.test_scenario}.py not found under ecloud/scenario_testing"
        logger.exception(error)

    if error is not None:
        try:
            testing_scenario = importlib.import_module(f"ecloud.scenario_testing.archived.{opt.test_scenario}")
        except ModuleNotFoundError:
            error = f"{opt.test_scenario}.py not found under ecloud/scenario_testing[/archived]"
            logger.exception(error)

    if opt.edge:
        yaml = f'ecloud/scenario_testing/config_yaml/{opt.test_scenario}_{opt.num_cars}_car.yaml'
    else:
        yaml = f'ecloud/scenario_testing/config_yaml/{opt.test_scenario}.yaml'
    config_yaml = os.path.join(os.path.dirname(os.path.realpath(__file__)), yaml)
    if not os.path.isfile(config_yaml):
        config_yaml = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                               f'ecloud/scenario_testing/config_yaml/archived/{opt.test_scenario}.yaml')
        if not os.path.isfile(config_yaml):
            error = f"ecloud/scenario_testing/config_yaml/[archived/]{yaml} not found!"
            logger.exception(error)

    return testing_scenario, config_yaml, error

def main():
    '''
    fetches the specific scenario module (or default) and calls its 'run_scenario' method
    '''
    opt = arg_parse()
    assert ( opt.apply_ml is True and opt.distributed == 0 ) or opt.apply_ml is False
    logger.debug(opt)

    print(f"eCloudSim Version: {__version__}")

    if opt.edge:
        assert opt.num_cars != 0
        assert opt.num_cars == 8 or opt.num_cars == 16 # add a new YAML otherwise
        opt.test_scenario = ecloud_globals.Consts.DEFAULT_EDGE_SCENARIO

    testing_scenario, config_yaml, error = get_scenario(opt)
    if error is not None:
        check_imports()
        sys.exit(error)

    if opt.build:
        subprocess.run(['python','-m','grpc_tools.protoc',
                        '-I./ecloud/protos','--python_out=.',
                        '--grpc_python_out=.','./ecloud//protos/ecloud.proto'],
                        check=True)

    EnvironmentConfig.set_environment(opt.environment)
    scenario_runner = getattr(testing_scenario, 'run_scenario')
    try:
        scenario_runner(opt, config_yaml)

    except KeyboardInterrupt:
        logger.info('exited by user.')
        sys.exit(0)

    except Exception as err: # pylint: disable=broad-exception-caught
        logger.exception("exception hit: %s - %s", type(err), err)
        raise

if __name__ == '__main__':
    main()
    