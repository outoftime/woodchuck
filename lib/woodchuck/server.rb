begin
  require 'logger'
  require 'eventmachine'
  require 'woodchuck/database'
rescue LoadError => e
  retry if require 'rubygems'
  raise e
end

module Woodchuck
  class Server < EM::P::HeaderAndContentProtocol
    def initialize
      @db = Database.new
      @log = Logger.new($stdout, Logger::DEBUG)
      super
    end

    def receive_request(headers_list, content)
      headers = {}
      headers_list.each do |header|
        name, value = header.split(/:\s*/, 2)
        headers[name] = value
      end
      case headers['Command']
      when 'ADD'
        id = @db.add(content)
        @log.debug("Added #{id}")
        send_data("Content-Length: 0\n")
        send_data("Id: #{id}\n\n")
      when 'UPDATE'
        @db.update(headers['Id'], content)
        send_data("Content-Length: 0\n\n")
      when 'GET'
        id = headers['Id']
        if doc = @db.get(id)
          @log.debug("Found #{id}.")
          send_data("Content-Length: #{doc.length}\n")
          send_data("Content-Type: text/json\n\n")
          send_data("#{doc}")
        else
          @log.debug("Not Found #{id}.")
          send_data("Content-Length: 0\n\n")
        end
      when 'MAP'
        @db.map(headers['Name'], content)
        send_data("Content-Length: 0\n\n")
      when 'TRUNCATE'
        @db.truncate
        send_data("Content-Length: 0\n\n")
      else
        send_data("Content-Length: 0\n")
        send_data("Error: Unknown command #{headers['Command']}\n\n")
      end
    rescue => e
      @log.error(e.message)
      e.backtrace.each { |line| @log.error(line) }
      send_data("Content-Length: 0\n")
      send_data("Error: #{e.message}\n\n")
    end
  end
end
