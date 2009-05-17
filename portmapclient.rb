require 'portmap'

SUNRPC::UDPClient.new(Portmap::PMAP_PROG, Portmap::PMAP_VERS, 1234
	'127.0.0.1') do |client|

	puts client.DUMP.inspect
	client.CALLIT({:prog => 1, :vers => 1, :proc => 1, :args=> ''})
end
