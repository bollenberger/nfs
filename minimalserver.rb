require 'minimal_prot'
require 'portmap_transport'

Thread.abort_on_exception = true

PortmapTransport::UDPServer.new(Minimal::MINIMAL_PROG, nil, '0.0.0.0', 1234).join
