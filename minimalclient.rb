require 'minimal_prot'
require 'portmap_transport'

PortmapTransport::UDPClient.new(Minimal::MINIMAL_PROG,
	Minimal::MINIMAL_VERSION, 1234, '127.0.0.1') do |client|

	10.times do
		puts client.REVERSE('Reverse this')
	end
end

