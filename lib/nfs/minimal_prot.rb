# A minimal SUNRPC protocol

require 'sunrpc'

module Minimal

include SUNRPC

MINIMAL_VERSION = 1

MINIMAL_PROG = Program.new 20000000 do
	version MINIMAL_VERSION do
		procedure String.new, :REVERSE, 1, String.new do |arg, cred, verf|
			puts arg
			arg.reverse
		end
	end
end

end
