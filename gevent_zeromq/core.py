"""This module wraps the :class:`Socket` and :class:`Context` found in :mod:`pyzmq <zmq>` to be non blocking
"""
import zmq
from zmq import *

# imported with different names as to not have the star import try to to clobber (when building with cython)
from zmq.core.context import Context as _original_Context
from zmq.core.socket import Socket as _original_Socket

from gevent.coros import RLock
from gevent.event import Event
from gevent.hub import get_hub

from gevent_zeromq.helpers import allow_unbound_disappear


class _Context(_original_Context):
    """Replacement for :class:`zmq.core.context.Context`

    Ensures that the greened Socket below is used in calls to `socket`.
    """

    def socket(self, socket_type):
        """Overridden method to ensure that the green version of socket is used

        Behaves the same as :meth:`zmq.core.context.Context.socket`, but ensures
        that a :class:`Socket` with all of its send and recv methods set to be
        non-blocking is returned
        """
        if self.closed:
            raise ZMQError(ENOTSUP)
        return _Socket(self, socket_type)

class _Socket(_original_Socket):
    """Green version of :class:`zmq.core.socket.Socket`

    The following methods are overridden:

        * send
        * send_multipart
        * recv
        * recv_multipart

    To ensure that the ``zmq.NOBLOCK`` flag is set and that sending or recieving
    is deferred to the hub if a ``zmq.EAGAIN`` (retry) error is raised.

    The `__state_changed` method is triggered when the zmq.FD for the socket is
    marked as readable and triggers the necessary read and write events (which
    are waited for in the recv and send methods).

    However, the readable status of zmq.FD is reset by send and recv, which
    introduces a potential race condition when a recv or send occurs after
    zmq.FD becomes readable and before the poll. To resolve this we wake waiting
    senders after a recv, and vice versa.

    `__send_lock` and `__recv_lock` are used to ensure that at most one greenlet
    is performing a `send/send_multipart` and `recv/recv_multipart`,
    respectively. This also ensures that at most one waiting greenlet is awoken
    by send and recv.

    getsockopt also consumes socket state change events, thus we also wake
    waiting senders and receivers after an invocation of getsockopt.

    Some doubleunderscore prefixes are used to minimize pollution of
    :class:`zmq.core.socket.Socket`'s namespace.
    """

    def __init__(self, context, socket_type):
        self._state_event = None
        super(_Socket, self).__init__(context, socket_type)
        self.__setup_events()
        self.__send_lock = RLock()
        self.__recv_lock = RLock()

    def __del__(self):
        """Unregisters itself from the event loop.
        """
        # We need __del__. We would not be able to access
        # the object properties inside close() called
        # from _original_Socket.__dealloc__().
        if self._state_event is not None:
            try:
                self._state_event.stop()
            except AttributeError, e:
                # gevent<1.0 compat
                self._state_event.cancel()
            self._state_event = None

    def close(self):
        # close the _state_event event, keeps the number of active file descriptors down
        if not self.closed and getattr(self, '_state_event', None):
            self.__del__()
        super(_Socket, self).close()

    def __setup_events(self):
        self.__readable = Event()
        self.__writable = Event()
        callback = allow_unbound_disappear(
                _Socket.__state_changed, self, _Socket)
        try:
            self._state_event = get_hub().loop.io(self.__getsockopt(FD), 1) # read state watcher
            self._state_event.start(callback)
        except AttributeError:
            # for gevent<1.0 compatibility
            from gevent.core import read_event
            self._state_event = read_event(self.__getsockopt(FD), callback, persist=True)

    def __state_changed(self, event=None, _evtype=None):
        if self.closed:
            # if the socket has entered a close state resume any waiting greenlets
            self.__writable.set()
            self.__readable.set()
            return

        events = self.__getsockopt(EVENTS)
        if events & POLLOUT:
            self.__writable.set()
        if events & POLLIN:
            self.__readable.set()

    def _wait_write(self):
        self.__writable.clear()
        self.__writable.wait()

    def _wait_read(self):
        self.__readable.clear()
        self.__readable.wait()

    def send(self, data, flags=0, copy=True, track=False):

        # if we're given the NOBLOCK flag act as normal and let the EAGAIN get raised
        if flags & NOBLOCK:
            # check if the send lock is taken in a non-blocking manner
            if not self.__send_lock.acquire(blocking=False):
                raise ZMQError(EAGAIN)
            self.__send_lock.release()
            return super(_Socket, self).send(data, flags, copy, track)

        # Lock to wait for send/send_multipart to complete. This will also ensure that at most
        # one greenlet at a time is waiting for a socket writable state change in case we get EAGAIN.
        with self.__send_lock:
            flags = flags | NOBLOCK
            while True: # Attempt to complete this operation indefinitely, blocking the current greenlet
                try:
                    return super(_Socket, self).send(data, flags, copy, track)
                except ZMQError, e:
                    if e.errno != EAGAIN:
                        raise
                finally:
                    # wake a waiting reader as the readable state may have changed and send consumes this event
                    self.__readable.set()
                # we got EAGAIN, wait for socket to change state
                self._wait_write()

    def send_multipart(self, msg_parts, flags=0, copy=True, track=False):
        # send_multipart is not greenlet-safe, i.e. message parts might get
        # split up if multiple greenlets call send and/or send_multipart on the same socket.
        # so we use a lock to ensure that there's only ony greenlet
        # calling send_multipart at any time.
        with self.__send_lock:
            return super(_Socket, self).send_multipart(msg_parts, flags, copy, track)

    def recv(self, flags=0, copy=True, track=False):

        # if we're given the NOBLOCK flag act as normal and let the EAGAIN get raised
        if flags & NOBLOCK:
            # check if the recv lock is taken in a non-blocking manner
            if not self.__recv_lock.acquire(blocking=False):
                raise ZMQError(EAGAIN)
            self.__recv_lock.release()
            return super(_Socket, self).recv(flags, copy, track)

        # Lock to wait for recv/recv_multipart to complete. This will also ensure that at most
        # one greenlet at a time is waiting for a socket readable state change in case we get EAGAIN.
        with self.__recv_lock:
            flags = flags | NOBLOCK
            while True: # Attempt to complete this operation indefinitely, blocking the current greenlet
                try:
                    return super(_Socket, self).recv(flags, copy, track)
                except ZMQError, e:
                    if e.errno != EAGAIN:
                        raise
                finally:
                    # wake a waiting writer as the writable state may have changed and recv consumes this event
                    self.__writable.set()
                # we got EAGAIN, wait for socket to change state
                self._wait_read()

    def recv_multipart(self, flags=0, copy=True, track=False):
        # recv_multipart is not greenlet-safe, i.e. message parts might get
        # split up if multiple greenlets call recv and/or recv_multipart on the same socket.
        # so we use a lock to ensure that there's only ony greenlet
        # calling recv_multipart at any time.
        with self.__recv_lock:
            return super(_Socket, self).recv_multipart(flags, copy, track)

    def __getsockopt(self, option):
        return super(_Socket, self).getsockopt(option)

    def getsockopt(self, option):
        try:
            return self.__getsockopt(option)
        finally:
            # wake a waiting reader and a writer as the writable/readable state may have changed and getsockopt consumes socket state change events
            self.__writable.set()
            self.__readable.set()
