# A Ruby SUNRPC implementation of portmap.
#
# Author: Brian Ollenberger

require 'nfs/sunrpc'

module Portmap

include SUNRPC

PMAP_PORT = 111

prot = Enumeration.new do
	name :TCP_IP, 6
	name :UDP_IP, 17
end

mapping = Structure.new do
	component :prog, UnsignedInteger.new
	component :vers, UnsignedInteger.new
	component :prot, prot
	component :port, UnsignedInteger.new
end

pmaplist = Structure.new do
	component :map, mapping
	component :next, Optional.new(self)
end

call_args = Structure.new do
	component :prog, UnsignedInteger.new
	component :vers, UnsignedInteger.new
	component :proc, UnsignedInteger.new
	component :args, Opaque.new
end

call_result = Structure.new do
	component :port, UnsignedInteger.new
	component :res, Opaque.new
end

# Port mapper procedures

PMAP_VERS = 2

PMAP_PROG = Program.new 100000 do
	version PMAP_VERS do
		procedure Boolean.new, :SET, 1, mapping
		procedure Boolean.new, :UNSET, 2, mapping
		procedure UnsignedInteger.new, :GETPORT, 3, mapping
		procedure Optional.new(pmaplist), :DUMP, 4, Void.new
		procedure call_result, :CALLIT, 5, call_args
	end
end

end
