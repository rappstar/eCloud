# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import sim_api_pb2 as sim__api__pb2


class OpenCDAStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.SimulationStateStream = channel.unary_stream(
                '/grpc.OpenCDA/SimulationStateStream',
                request_serializer=sim__api__pb2.Empty.SerializeToString,
                response_deserializer=sim__api__pb2.SimulationState.FromString,
                )
        self.SendUpdate = channel.unary_unary(
                '/grpc.OpenCDA/SendUpdate',
                request_serializer=sim__api__pb2.VehicleUpdate.SerializeToString,
                response_deserializer=sim__api__pb2.Empty.FromString,
                )


class OpenCDAServicer(object):
    """Missing associated documentation comment in .proto file."""

    def SimulationStateStream(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SendUpdate(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_OpenCDAServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'SimulationStateStream': grpc.unary_stream_rpc_method_handler(
                    servicer.SimulationStateStream,
                    request_deserializer=sim__api__pb2.Empty.FromString,
                    response_serializer=sim__api__pb2.SimulationState.SerializeToString,
            ),
            'SendUpdate': grpc.unary_unary_rpc_method_handler(
                    servicer.SendUpdate,
                    request_deserializer=sim__api__pb2.VehicleUpdate.FromString,
                    response_serializer=sim__api__pb2.Empty.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'grpc.OpenCDA', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class OpenCDA(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def SimulationStateStream(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/grpc.OpenCDA/SimulationStateStream',
            sim__api__pb2.Empty.SerializeToString,
            sim__api__pb2.SimulationState.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def SendUpdate(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/grpc.OpenCDA/SendUpdate',
            sim__api__pb2.VehicleUpdate.SerializeToString,
            sim__api__pb2.Empty.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
