require 'eventmachine'
require 'json'

module Woodchuck
  class Client
    RequestError = Class.new(StandardError)

    def initialize(host = 'localhost', port = 9012)
      @socket = TCPSocket.new(host, port)
    end

    def add(doc)
      headers, content = request(JSON.dump(doc), 'Command' => 'ADD')
      headers['Id'].to_i
    end

    def update(id, doc)
      request(JSON.dump(doc, 'Command' => 'UPDATE', 'Id' => id))
    end

    def get(id)
      headers, content = request(nil, 'Command' => 'GET', 'Id' => id)
      JSON.parse(content) unless content.empty?
    end

    def map(name, function_definition)
      headers, content =
        request(function_definition, 'Command' => 'MAP', 'Name' => name)
    end

    def truncate
      request(nil, 'Command' => 'TRUNCATE')
    end

    private

    def request(content, headers)
      headers.each_pair { |name, value| @socket.puts("#{name}: #{value}") }
      if content then @socket.puts "Content-Length: #{content.length}"
      else @socket.puts "Content-Length: 0"
      end
      @socket.puts("")
      @socket.puts(content) if content
      response
    end

    def response
      headers, content = {}, ''
      loop do
        line = @socket.gets
        break if line =~ /^\s*[\r\n]*$/
        name, value = line.split(/:\s*/)
        headers[name] = value
      end
      length = headers['Content-Length'].to_i
      content << @socket.readchar while content.length < length
      if e = headers['Error']
        raise RequestError, e
      end
      [headers, content]
    end
  end
end
