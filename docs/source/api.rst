h2 API
======

This document details the API of h2.

Semantic Versioning
-------------------

h2 follows semantic versioning for its public API. Please note that the
guarantees of semantic versioning apply only to the API that is *documented
here*. Simply because a method or data field is not prefaced by an underscore
does not make it part of h2's public API. Anything not documented here is
subject to change at any time.

Connection
----------

.. autoclass:: h2.connection.H2Connection
   :members:
   :exclude-members: inbound_flow_control_window


Configuration
-------------

.. autoclass:: h2.config.H2Configuration
   :members:


.. _h2-events-api:

Events
------

.. autoclass:: h2.events.RequestReceived
   :members:

.. autoclass:: h2.events.ResponseReceived
   :members:

.. autoclass:: h2.events.TrailersReceived
   :members:

.. autoclass:: h2.events.InformationalResponseReceived
   :members:

.. autoclass:: h2.events.DataReceived
   :members:

.. autoclass:: h2.events.WindowUpdated
   :members:

.. autoclass:: h2.events.RemoteSettingsChanged
   :members:

.. autoclass:: h2.events.PingReceived
   :members:

.. autoclass:: h2.events.PingAckReceived
   :members:

.. autoclass:: h2.events.StreamEnded
   :members:

.. autoclass:: h2.events.StreamReset
   :members:

.. autoclass:: h2.events.PushedStreamReceived
   :members:

.. autoclass:: h2.events.SettingsAcknowledged
   :members:

.. autoclass:: h2.events.PriorityUpdated
   :members:

.. autoclass:: h2.events.ConnectionTerminated
   :members:

.. autoclass:: h2.events.AlternativeServiceAvailable
   :members:

.. autoclass:: h2.events.UnknownFrameReceived
   :members:


Exceptions
----------

.. autoclass:: h2.exceptions.H2Error
   :members:

.. autoclass:: h2.exceptions.NoSuchStreamError
   :show-inheritance:
   :members:

.. autoclass:: h2.exceptions.StreamClosedError
   :show-inheritance:
   :members:

.. autoclass:: h2.exceptions.RFC1122Error
   :show-inheritance:
   :members:


Protocol Errors
~~~~~~~~~~~~~~~

.. autoclass:: h2.exceptions.ProtocolError
   :show-inheritance:
   :members:

.. autoclass:: h2.exceptions.FrameTooLargeError
   :show-inheritance:
   :members:

.. autoclass:: h2.exceptions.FrameDataMissingError
   :show-inheritance:
   :members:

.. autoclass:: h2.exceptions.TooManyStreamsError
   :show-inheritance:
   :members:

.. autoclass:: h2.exceptions.FlowControlError
   :show-inheritance:
   :members:

.. autoclass:: h2.exceptions.StreamIDTooLowError
   :show-inheritance:
   :members:

.. autoclass:: h2.exceptions.InvalidSettingsValueError
   :members:

.. autoclass:: h2.exceptions.NoAvailableStreamIDError
   :show-inheritance:
   :members:

.. autoclass:: h2.exceptions.InvalidBodyLengthError
   :show-inheritance:
   :members:

.. autoclass:: h2.exceptions.UnsupportedFrameError
   :members:

.. autoclass:: h2.exceptions.DenialOfServiceError
   :show-inheritance:
   :members:

.. autoclass:: h2.exceptions.TruncatedFrameError
   :show-inheritance:
   :members:

.. autoclass:: h2.exceptions.BoundaryViolationError
   :show-inheritance:
   :members:


Boundary and Ownership Diagnostics
----------------------------------

.. autoclass:: h2.boundary.BodyBoundary
   :members:

.. autoclass:: h2.boundary.UpstreamRemainder
   :members:

.. autoclass:: h2.boundary.BoundaryEvent
   :members:

.. autoclass:: h2.boundary.BoundaryEventLog
   :members:

.. autofunction:: h2.boundary.classify_failure

.. autoclass:: h2.frame_buffer.FrameBufferState
   :members:

Connection Reuse Pool
---------------------

.. autoclass:: h2.pool.ConnectionPool
   :members:

.. autoclass:: h2.pool.PoolEvent
   :members:

.. autofunction:: h2.pool.assert_reusable

Replay Harness
--------------

.. autoclass:: h2.diagnostics.ReplayResult
   :members:

.. autoclass:: h2.diagnostics.ComparisonResult
   :members:

.. autoclass:: h2.diagnostics.Divergence
   :members:

.. autoclass:: h2.diagnostics.ReplayRecord
   :members:

.. autofunction:: h2.diagnostics.replay

.. autofunction:: h2.diagnostics.compare_schedules


HTTP/2 Error Codes
------------------

.. automodule:: h2.errors
   :members:


Settings
--------

.. autoclass:: h2.settings.SettingCodes
   :members:

.. autoclass:: h2.settings.Settings
   :inherited-members:

.. autoclass:: h2.settings.ChangedSetting
   :members:
