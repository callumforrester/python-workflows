import functools
from abc import ABC, abstractmethod
from inspect import Parameter, signature
from typing import Any, Callable, Mapping, Optional, Type

from pydantic import BaseModel

from workflows.transport.common_transport import MessageCallback, TemporarySubscription

from . import BaseTransportMiddleware

ModelMessageCallback = Callable[[Mapping[str, Any], BaseModel], None]


class BaseConverter(ABC):
    @abstractmethod
    def serialize_message(self, message: Any) -> Mapping[str, Any]:
        ...

    @abstractmethod
    def deserialize_message(self, message: Mapping[str, Any], expected_type: Type[Any]):
        ...


class PydanticConverter(BaseConverter):
    def serialize_message(self, message: Any) -> Mapping[str, Any]:
        if isinstance(message, BaseModel):
            return message.dict()
        else:
            return message

    def deserialize_message(self, message: Mapping[str, Any], expected_type: Type[Any]):
        if BaseModel in expected_type.mro():
            return expected_type(**message)
        else:
            return message


class ConvertingMiddleware(BaseTransportMiddleware):
    _converter: BaseConverter

    def __init__(self, converter: Optional[BaseConverter] = None) -> None:
        if converter is None:
            converter = PydanticConverter()
        self._converter = converter
        super().__init__()

    def send(self, call_next: Callable, destination, message, **kwargs):
        to_send = self._converter.serialize_message(message)
        return call_next(destination, to_send, **kwargs)

    def subscribe(self, call_next: Callable, channel, callback, **kwargs) -> int:
        return call_next(channel, self._deserailizing_wrapper(callback), **kwargs)

    def subscribe_broadcast(
        self, call_next: Callable, channel, callback, **kwargs
    ) -> int:
        return call_next(channel, self._deserailizing_wrapper(callback), **kwargs)

    def subscribe_temporary(
        self,
        call_next: Callable,
        channel_hint: Optional[str],
        callback: MessageCallback,
        **kwargs,
    ) -> TemporarySubscription:
        return call_next(channel_hint, self._deserailizing_wrapper(callback), **kwargs)

    def _deserailizing_wrapper(
        self,
        subscription_callback: ModelMessageCallback,
    ) -> MessageCallback:
        expected_type = ConvertingMiddleware.get_message_type(subscription_callback)

        @functools.wraps(subscription_callback)
        def wrapped_callback(header: Mapping[str, Any], message: Mapping[str, Any]):
            deserailized_message = self._converter.deserialize_message(
                message, expected_type
            )
            return subscription_callback(header, deserailized_message)

        return wrapped_callback

    @staticmethod
    def get_message_type(
        subscription_callback: ModelMessageCallback,
    ) -> Type[Any]:
        params = list(signature(subscription_callback).parameters.values())
        assert len(params) == 2, "Subscription callback signature must have 2 arguments"

        message_param = params[1]
        expected_type = message_param.annotation
        if expected_type is Parameter.empty:
            expected_type = Any
        return expected_type
